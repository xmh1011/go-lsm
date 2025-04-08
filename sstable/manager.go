package sstable

import (
	"container/list"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/memtable"
	"github.com/xmh1011/go-lsm/util"
)

// Manager 管理内存中的 SSTable (Footer/Filter/Index) + 磁盘中剩余的文件
type Manager struct {
	mu sync.RWMutex

	// LRU管理:
	lru   *list.List               // *list.Element.Value => *SSTable
	cache map[string]*list.Element // filePath => element

	// DiskMap: 不在内存缓存的文件
	DiskMap map[int][]string
	// TotalMap: 全部文件记录 (内存 + 磁盘)
	TotalMap map[int][]string
}

const (
	maxSSTableCount = 100
	minSSTableLevel = 0
	maxSSTableLevel = 6
	levelSizeBase   = 2
)

func init() {
	// 创建 SSTable 目录
	err := os.MkdirAll(config.GetSSTablePath(), os.ModePerm)
	if err != nil {
		log.Errorf("failed to create sstable directory: %v", err)
	}

	for i := minSSTableLevel; i <= maxSSTableLevel; i++ {
		err := os.MkdirAll(sstableLevelPath(i, config.GetSSTablePath()), os.ModePerm)
		if err != nil {
			log.Errorf("failed to create sstable level %d directory: %v", i, err)
		}
	}
}

func NewSSTableManager() *Manager {
	return &Manager{
		lru:      list.New(),
		cache:    make(map[string]*list.Element),
		DiskMap:  make(map[int][]string),
		TotalMap: make(map[int][]string),
	}
}

func (m *Manager) CreateNewSSTable(imem *memtable.IMemtable) error {
	sst := BuildSSTableFromIMemtable(imem)
	defer imem.Clean() // 删除 wal 文件

	// 最新的 SSTable 直接写入 level-0
	filePath := sstableFilePath(sst.id, sst.level, config.GetSSTablePath())
	err := sst.EncodeTo(filePath)
	if err != nil {
		log.Errorf("encode sstable to file %s error: %s", sst.FilePath(), err.Error())
		return err
	}
	// 清空 data block，将新表加入 LRU
	sst.DataBlocks = nil
	m.AddTable(sst)
	// 插入 totalMap
	m.addNewFile(sst.level, filePath)

	// 更新多级的 sstable
	if err := m.Compaction(); err != nil {
		log.Errorf("compaction error: %s", err.Error())
		return err
	}

	return nil
}

// AddTable inserts or promotes a table into LRU (most recently used).
func (m *Manager) AddTable(table *SSTable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fp := table.FilePath()

	// 如果已在 LRU，移动到前面
	if elem, ok := m.cache[fp]; ok {
		m.lru.MoveToFront(elem)
		return
	}

	// 否则新插入
	e := m.lru.PushFront(table)
	m.cache[fp] = e

	// 超过上限，则移除尾部
	for m.lru.Len() > maxSSTableCount {
		tail := m.lru.Back()
		oldTable := tail.Value.(*SSTable)
		oldFile := oldTable.FilePath()

		// 加回 DiskMap
		m.DiskMap[oldTable.level] = append(m.DiskMap[oldTable.level], oldFile)

		// 从 LRU 删除
		delete(m.cache, oldFile)
		m.lru.Remove(tail)
	}
}

// Search 会先在内存(LRU)中查找，不命中再去 DiskMap
func (m *Manager) Search(key kv.Key) ([]byte, error) {
	// 先从最低的 level 开始查找
	for i := minSSTableLevel; i <= maxSSTableLevel; i++ {
		files := m.getSortedFilesByLevel(i)
		n := len(files)
		for j := 0; j < len(files); j++ {
			sst, ok := m.isFileInMemoryAndReturn(files[n-1-j])
			if ok { // 如果在内存中
				val, err := m.SearchFromTable(sst, key)
				if err != nil {
					log.Errorf("search from table error: %s", err.Error())
					return nil, err
				}
				if val != nil {
					return val, nil
				}
			} else { // 如果不在内存中
				sst := NewSSTable()
				err := sst.LoadMetaBlockToMemory(files[n-1-j])
				if err != nil {
					log.Errorf("decode metadata error: %s", err.Error())
					return nil, err
				}
				val, err := m.SearchFromTable(sst, key)
				if err != nil {
					log.Errorf("search from table error: %s", err.Error())
					return nil, err
				}
				if val != nil {
					// 命中后添加到 LRU
					m.AddTable(sst)
					// 在 diskMap 中删除
					m.removeFromDiskMap(sst.FilePath(), sst.level)
					return val, nil
				}
			}
		}
	}

	return nil, nil
}

func (m *Manager) SearchFromTable(sst *SSTable, key kv.Key) (kv.Value, error) {
	// 布隆过滤器先粗判
	if !sst.MayContain(key) {
		return nil, nil
	}

	var err error
	sst.file, err = os.Open(sst.FilePath())
	if err != nil {
		log.Errorf("open file %s error: %s", sst.FilePath(), err.Error())
		return nil, err
	}
	it := NewSSTableIterator(sst)
	it.Seek(key)
	if it.Valid() && it.Key() == key {
		val := it.Value()
		sst.Close()

		// 命中后提升为 LRU 的 MRU（需要写锁）
		m.mu.Lock()
		if elem, ok := m.cache[sst.FilePath()]; ok {
			m.lru.MoveToFront(elem)
		}
		m.mu.Unlock()

		return val, nil
	}
	sst.Close()

	return nil, nil
}

// Recover 从指定路径恢复 sstable 的元数据，注意仅加载元数据（不加载 datablock）。
// 恢复时依次将文件中的信息加载到内存 LRU 队列中（不会超过队列限制），
// 并且将文件信息按照 level 记录到 TotalMap 中（同时如果加载后超出 LRU 限制，
// 被移除的表会进入 DiskMap）。
func (m *Manager) Recover() error {
	var maxID uint64
	for i := minSSTableLevel; i <= maxSSTableLevel; i++ {
		dir := sstableLevelPath(i, config.GetSSTablePath())
		files, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				log.Debugf("directory %s does not exist, skipping", dir)
				continue
			}
			log.Errorf("failed to read directory %s: %v", dir, err)
			return err
		}
		if len(files) == 0 {
			log.Debugf("directory %s is empty, skipping", dir)
			continue
		}
		// 对文件进行排序
		sort.Slice(files, func(i, j int) bool {
			return util.ExtractID(files[i].Name()) < util.ExtractID(files[j].Name())
		})
		// 取出最新的文件 ID
		latestID := util.ExtractID(files[len(files)-1].Name())
		if latestID > maxID {
			maxID = latestID
		}

		// 遍历所有文件，对每个文件进行处理
		for _, file := range files {
			if file.IsDir() {
				continue
			}

			// 拼接得到完整的文件路径
			filePath := filepath.Join(dir, file.Name())
			// 创建一个新的 SSTable 实例（仅加载元数据信息，不加载 data block）
			table := NewSSTable()
			if err := table.LoadMetaBlockToMemory(filePath); err != nil {
				log.Errorf("Recover: load meta for file %s error: %s", filePath, err.Error())
				return err
			}

			// 将该文件记录到 TotalMap 中（全部文件的记录）
			m.addNewFile(table.level, filePath)

			// 添加到 LRU 中，如果超过容量，AddTable 内部会将 LRU 尾部的表挪到 DiskMap 中
			m.AddTable(table)
		}
	}
	// 处理自增 id 的逻辑
	idGenerator.Add(maxID)

	return nil
}

// removeInMemory removes an in-memory table from LRU & cache, O(1).
// 合并(compaction)后可删除旧 sstable
func (m *Manager) removeInMemory(filePath string, level int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if elem, ok := m.cache[filePath]; ok {
		m.lru.Remove(elem)
		delete(m.cache, filePath)

		// 放回 diskMap
		m.DiskMap[level] = append(m.DiskMap[level], filePath)
	}
}

// removeFromDiskMap removes a file from DiskMap.
func (m *Manager) removeFromDiskMap(filePath string, level int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DiskMap[level] = util.RemoveString(m.DiskMap[level], filePath)
}

// promoteElement 将一个已知list.Element移到队首 (O(1))
func (m *Manager) promoteElement(elem *list.Element) {
	if elem == m.lru.Front() {
		return
	}
	m.lru.MoveToFront(elem)
}

// isFileInMemory 判断某个sstable文件是否已经在内存中
func (m *Manager) isFileInMemory(filePath string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	elem, ok := m.cache[filePath]
	if !ok || elem == nil {
		return false
	}

	_, ok = elem.Value.(*SSTable)
	return ok
}

func (m *Manager) isFileInMemoryAndReturn(filePath string) (*SSTable, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	elem, ok := m.cache[filePath]
	if !ok || elem == nil {
		return nil, false
	}
	sst, ok := elem.Value.(*SSTable)

	return sst, ok
}

func (m *Manager) findFileToBeMergedByLevel(level int) string {
	return util.GetOldestSSTableFile(m.getFilesByLevel(level))
}

func (m *Manager) isLevelNeedToBeMerged(level int) bool {
	return len(m.getFilesByLevel(level)) > maxFileNumsInLevel(level)
}

// getFilesByLevel 返回磁盘中某层的所有文件
func (m *Manager) getFilesByLevel(level int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return append([]string{}, m.TotalMap[level]...)
}

// getSortedFilesByLevel 返回磁盘中某层的所有文件，按照 id（时间戳）的大小排序
func (m *Manager) getSortedFilesByLevel(level int) []string {
	files := m.getFilesByLevel(level)
	sort.Slice(files, func(i, j int) bool {
		return util.ExtractID(files[i]) < util.ExtractID(files[j])
	})
	return files
}

func (m *Manager) addNewFile(level int, filePath string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalMap[level] = append(m.TotalMap[level], filePath)
}

// getAll 返回LRU中的所有SSTable
func (m *Manager) getAll() []*SSTable {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]*SSTable, 0, m.lru.Len())
	for e := m.lru.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(*SSTable))
	}
	return out
}

func maxFileNumsInLevel(level int) int {
	return int(math.Pow(float64(levelSizeBase), float64(level+2)))
}
