package sstable

import (
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
	// 为各层创建对应目录
	for i := minSSTableLevel; i <= maxSSTableLevel; i++ {
		err := os.MkdirAll(sstableLevelPath(i, config.GetSSTablePath()), os.ModePerm)
		if err != nil {
			log.Errorf("failed to create sstable level %d directory: %v", i, err)
		}
	}
}

// Manager 管理内存中的 SSTable 元信息（Footer/Filter/Index）+ 磁盘中剩余的文件记录。
type Manager struct {
	mu sync.RWMutex

	// metas 保存所有保留在内存中的 SSTable 元信息，按 sst.id 降序排序（最新的在最前）
	metas []*SSTable

	indexMap map[string]int // 用于快速查找 SSTable 的索引

	// diskMap：不在内存中缓存的文件路径（按层级分布）
	diskMap map[int][]string
	// totalMap：全部 SSTable 文件路径记录（内存+磁盘）
	totalMap map[int][]string

	// 异步合并等待（Level1 及以上）
	compactionCond  *sync.Cond // 条件变量，用于等待异步合并完成
	compacting      bool       // 标识是否正在异步合并
	compactingLevel int        // 正在合并的层级，0 表示 Level0，-1 表示无
}

func NewSSTableManager() *Manager {
	mgr := &Manager{
		metas:           make([]*SSTable, 0),
		diskMap:         make(map[int][]string),
		totalMap:        make(map[int][]string),
		indexMap:        make(map[string]int),
		compacting:      false,
		compactingLevel: -1,
	}
	mgr.compactionCond = sync.NewCond(&mgr.mu)
	return mgr
}

// CreateNewSSTable 将 imem 数据构建为 SSTable，写入到磁盘，然后将其元数据添加到内存中（按照 id 降序保存）。
func (m *Manager) CreateNewSSTable(imem *memtable.IMemtable) error {
	sst := BuildSSTableFromIMemtable(imem)
	defer imem.Clean() // 删除 WAL 文件

	// 写入 Level0 文件
	filePath := sstableFilePath(sst.id, sst.level, config.GetSSTablePath())
	err := sst.EncodeTo(filePath)
	if err != nil {
		log.Errorf("encode sstable to file %s error: %s", sst.FilePath(), err.Error())
		return err
	}
	sst.DataBlocks = nil

	// 添加到内存中（按照 id 降序排序，只保留最新 100 个）
	m.addTable(sst)
	// 同时记录到 totalMap
	m.addNewFile(sst.level, filePath)

	// 执行合并逻辑
	if err := m.Compaction(); err != nil {
		log.Errorf("compaction error: %s", err.Error())
		return err
	}

	return nil
}

// Search 遍历所有层（从 Level0 到 Level6）查找 key。
// 如果查询的层 (Level1 及以上)正处于异步合并状态，则等待合并完成后再查询。
func (m *Manager) Search(key kv.Key) ([]byte, error) {
	// 逐层查找
	for level := minSSTableLevel; level <= maxSSTableLevel; level++ {
		// 对 Level1 及以上，如果该层正在异步合并，则等待结束
		if level >= 1 {
			m.mu.Lock()
			for m.compacting && m.compactingLevel <= level {
				m.compactionCond.Wait()
			}
			m.mu.Unlock()
		}
		files := m.getSortedFilesByLevel(level)
		n := len(files)
		for j := 0; j < n; j++ {
			filePath := files[n-1-j]
			sst, ok := m.isFileInMemoryAndReturn(filePath)
			if !ok {
				sst = NewSSTable()
				if err := sst.DecodeMetaData(filePath); err != nil {
					log.Errorf("decode metadata error: %s", err.Error())
					return nil, err
				}
			}
			val, err := m.SearchFromTable(sst, key)
			if err != nil {
				log.Errorf("search from table error: %s", err.Error())
				return nil, err
			}
			if !ok { // 如果是从磁盘加载的文件，则关闭文件句柄
				sst.Close()
			}
			if val != nil {
				return val, nil
			}
		}
	}
	return nil, nil
}

func (m *Manager) SearchFromTable(sst *SSTable, key kv.Key) (kv.Value, error) {
	if !sst.MayContain(key) {
		return nil, nil
	}
	it := NewSSTableIterator(sst)
	it.Seek(key)
	if it.Valid() && it.Key() == key {
		val := it.Value()
		return val, nil
	}

	return nil, nil
}

// Recover 加载所有层中 SSTable 的元数据信息到内存中，并记录在 totalMap 中。
// 注意：只加载元数据信息，不加载 data block。
func (m *Manager) Recover() error {
	var maxID uint64
	for i := minSSTableLevel; i <= maxSSTableLevel; i++ {
		dir := sstableLevelPath(i, config.GetSSTablePath())
		files, err := os.ReadDir(dir)
		if err != nil {
			log.Errorf("failed to read directory %s: %v", dir, err)
			return err
		}
		if len(files) == 0 {
			log.Debugf("directory %s is empty, skipping", dir)
			continue
		}
		sort.Slice(files, func(i, j int) bool {
			return util.ExtractID(files[i].Name()) < util.ExtractID(files[j].Name())
		})
		latestID := util.ExtractID(files[len(files)-1].Name())
		if latestID > maxID {
			maxID = latestID
		}
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			filePath := filepath.Join(dir, file.Name())
			table := NewRecoverSSTable(i)
			table.id = util.ExtractID(file.Name())
			if err := table.DecodeMetaData(filePath); err != nil {
				log.Errorf("Recover: load meta for file %s error: %s", filePath, err.Error())
				return err
			}
			m.addNewFile(table.level, filePath)
			m.addTable(table)
		}
	}
	idGenerator.Add(maxID)
	return nil
}

// addTable 将新的 SSTable 元数据插入到内存中，保证 metas 按 sst.id 降序排序，
// 并只保留前 maxSSTableCount 个文件。
func (m *Manager) addTable(table *SSTable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := sort.Search(len(m.metas), func(i int) bool {
		if m.metas[i].level > table.level {
			return true
		}
		if m.metas[i].level < table.level {
			return false
		}
		return m.metas[i].id < table.id
	})

	// 插入 metas
	m.metas = append(m.metas, nil)
	copy(m.metas[idx+1:], m.metas[idx:])
	m.metas[idx] = table

	// 更新 indexMap（注意：插入后，所有 index >= idx 的都要加1）
	for path, i := range m.indexMap {
		if i >= idx {
			m.indexMap[path] = i + 1
		}
	}
	m.indexMap[table.FilePath()] = idx

	// 超限移除最后一个
	if len(m.metas) > maxSSTableCount {
		removed := m.metas[len(m.metas)-1]
		delete(m.indexMap, removed.FilePath())
		m.metas = m.metas[:len(m.metas)-1]
		m.diskMap[removed.level] = append(m.diskMap[removed.level], removed.FilePath())
		removed.Close()
	}
}

// removeOldSSTables 用于删除指定旧文件的内存和磁盘信息
func (m *Manager) removeOldSSTables(oldFiles []string, level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, oldPath := range oldFiles {
		idx, ok := m.indexMap[oldPath]
		if !ok || idx >= len(m.metas) {
			continue
		}

		// 删除 metas[idx]
		m.metas = append(m.metas[:idx], m.metas[idx+1:]...)
		delete(m.indexMap, oldPath)

		// 调整 indexMap 中所有大于 idx 的项索引
		for path, i := range m.indexMap {
			if i > idx {
				m.indexMap[path] = i - 1
			}
		}

		m.diskMap[level] = util.RemoveString(m.diskMap[level], oldPath)
		m.totalMap[level] = util.RemoveString(m.totalMap[level], oldPath)
		// 物理删除文件
		if err := os.Remove(oldPath); err != nil {
			log.Errorf("remove file %s error: %s", oldPath, err.Error())
			return err
		}
	}

	return nil
}

// addNewSSTables 用于将新表加入内存和对应 level
func (m *Manager) addNewSSTables(newTables []*SSTable, level int) error {
	for _, nt := range newTables {
		// 写入磁盘
		if err := nt.EncodeTo(nt.FilePath()); err != nil {
			log.Errorf("encode sstable to file %s error: %s", nt.FilePath(), err.Error())
			return err
		}
		nt.DataBlocks = nil
		// 使用 addTable 插入到内存中，保持 metas 排序与大小限制
		m.addTable(nt)
		m.addNewFile(level, nt.FilePath())
	}

	return nil
}

func (m *Manager) removeFromDiskMap(filePath string, level int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.diskMap[level] = util.RemoveString(m.diskMap[level], filePath)
}

func (m *Manager) addNewFile(level int, filePath string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalMap[level] = append(m.totalMap[level], filePath)
}

// isFileInMemory 遍历内存中 metas 判断某个文件是否存在
func (m *Manager) isFileInMemory(filePath string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.indexMap[filePath]
	return ok
}

// isFileInMemoryAndReturn 判断并返回内存中对应的 SSTable
func (m *Manager) isFileInMemoryAndReturn(filePath string) (*SSTable, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	idx, ok := m.indexMap[filePath]
	if !ok || idx >= len(m.metas) {
		return nil, false
	}
	return m.metas[idx], true
}

func (m *Manager) getFilesByLevel(level int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return append([]string{}, m.totalMap[level]...)
}

func (m *Manager) isLevelNeedToBeMerged(level int) bool {
	return len(m.getFilesByLevel(level)) > maxFileNumsInLevel(level)
}

// getSortedFilesByLevel 返回磁盘中某层的所有文件，按照 id（时间戳）的大小排序
func (m *Manager) getSortedFilesByLevel(level int) []string {
	files := m.getFilesByLevel(level)
	sort.Slice(files, func(i, j int) bool {
		return util.ExtractID(files[i]) < util.ExtractID(files[j])
	})

	return files
}

// getAll 返回内存中所有 SSTable 元数据的副本
func (m *Manager) getAll() []*SSTable {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cpy := make([]*SSTable, len(m.metas))
	copy(cpy, m.metas)
	return cpy
}

func maxFileNumsInLevel(level int) int {
	return int(math.Pow(float64(levelSizeBase), float64(level+2)))
}
