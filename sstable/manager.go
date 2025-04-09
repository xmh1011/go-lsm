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

	// DiskMap：不在内存中缓存的文件路径（按层级分布）
	DiskMap map[int][]string
	// TotalMap：全部 SSTable 文件路径记录（内存+磁盘）
	TotalMap map[int][]string

	// 异步合并等待（Level1 及以上）
	compactionCond  *sync.Cond // 条件变量，用于等待异步合并完成
	compacting      bool       // 标识是否正在异步合并
	compactingLevel int        // 正在合并的层级，0 表示 Level0，-1 表示无
}

func NewSSTableManager() *Manager {
	mgr := &Manager{
		metas:           make([]*SSTable, 0),
		DiskMap:         make(map[int][]string),
		TotalMap:        make(map[int][]string),
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
	sst.filePath = filePath

	// 添加到内存中（按照 id 降序排序，只保留最新 100 个）
	m.addTable(sst)
	// 同时记录到 TotalMap
	m.addNewFile(sst.level, filePath)

	// 执行合并逻辑
	if err := m.Compaction(); err != nil {
		log.Errorf("compaction error: %s", err.Error())
		return err
	}

	return nil
}

// addTable 将新的 SSTable 元数据插入到内存中，保证 metas 按 sst.id 降序排序，
// 并只保留前 maxSSTableCount 个文件。
func (m *Manager) addTable(table *SSTable) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 使用二分搜索在 metas 中找到插入位置，保持降序（最新 id 最大）
	idx := sort.Search(len(m.metas), func(i int) bool {
		// 若 metas[i].id < table.id，说明 table 应在前面
		return m.metas[i].id < table.id
	})
	// 插入新元素
	m.metas = append(m.metas, nil)
	copy(m.metas[idx+1:], m.metas[idx:])
	m.metas[idx] = table

	// 超过上限则删除末尾（最旧）的项
	if len(m.metas) > maxSSTableCount {
		removed := m.metas[len(m.metas)-1]
		m.metas = m.metas[:len(m.metas)-1]
		// 放回到 DiskMap 中
		m.DiskMap[removed.level] = append(m.DiskMap[removed.level], removed.FilePath())
	}
}

// Search 遍历所有层（从 Level0 到 Level6）查找 key。
// 如果查询的层 (Level1 及以上)正处于异步合并状态，则等待合并完成后再查询。
func (m *Manager) Search(key kv.Key) ([]byte, error) {
	// 逐层查找
	for level := minSSTableLevel; level <= maxSSTableLevel; level++ {
		// 对 Level1 及以上，如果该层正在异步合并，则等待结束
		if level >= 1 {
			m.mu.Lock()
			for m.compacting && m.compactingLevel == level {
				m.compactionCond.Wait()
			}
			m.mu.Unlock()
		}
		files := m.getSortedFilesByLevel(level)
		n := len(files)
		for j := 0; j < n; j++ {
			filePath := files[n-1-j]
			var sst *SSTable
			if inMem, ok := m.isFileInMemoryAndReturn(filePath); ok {
				sst = inMem
			} else {
				sst = NewSSTable()
				if err := sst.LoadMetaBlockToMemory(filePath); err != nil {
					log.Errorf("decode metadata error: %s", err.Error())
					return nil, err
				}
			}
			val, err := m.SearchFromTable(sst, key)
			if err != nil {
				log.Errorf("search from table error: %s", err.Error())
				return nil, err
			}
			if val != nil {
				if !m.isFileInMemory(sst.FilePath()) {
					m.addTable(sst)
					m.removeFromDiskMap(sst.FilePath(), sst.level)
				}
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
	file, err := os.Open(sst.FilePath())
	if err != nil {
		log.Errorf("open file %s error: %s", sst.FilePath(), err.Error())
		return nil, err
	}
	sst.file = file
	it := NewSSTableIterator(sst)
	it.Seek(key)
	if it.Valid() && it.Key() == key {
		val := it.Value()
		sst.Close()
		return val, nil
	}
	sst.Close()
	return nil, nil
}

// Recover 加载所有层中 SSTable 的元数据信息到内存中，并记录在 TotalMap 中。
// 注意：只加载元数据信息，不加载 data block。
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
			table := NewSSTable()
			if err := table.LoadMetaBlockToMemory(filePath); err != nil {
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

func (m *Manager) removeFromDiskMap(filePath string, level int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DiskMap[level] = util.RemoveString(m.DiskMap[level], filePath)
}

func (m *Manager) addNewFile(level int, filePath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TotalMap[level] = append(m.TotalMap[level], filePath)
}

// isFileInMemory 遍历内存中 metas 判断某个文件是否存在
func (m *Manager) isFileInMemory(filePath string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sst := range m.metas {
		if sst.FilePath() == filePath {
			return true
		}
	}
	return false
}

// isFileInMemoryAndReturn 判断并返回内存中对应的 SSTable
func (m *Manager) isFileInMemoryAndReturn(filePath string) (*SSTable, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sst := range m.metas {
		if sst.FilePath() == filePath {
			return sst, true
		}
	}
	return nil, false
}

func (m *Manager) getFilesByLevel(level int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return append([]string{}, m.TotalMap[level]...)
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
