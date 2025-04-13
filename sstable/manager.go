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
	"github.com/xmh1011/go-lsm/sstable/block"
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

type FileInfo struct {
	filePath string
	minKey   kv.Key
}

// Manager 管理内存中的 SSTable 元信息（Footer/Filter/Index）+ 磁盘中剩余的文件记录。
type Manager struct {
	mu sync.Mutex

	// metas 保存所有保留在内存中的 SSTable 元信息，按 sst.id 降序排序（最新的在最前）
	metas []*SSTable

	indexMap map[string]int // 用于快速查找 SSTable 的索引

	// 稀疏索引，用于记录 level 1 及以上的文件的索引信息
	// 通过二分查找，快速定位 key 可能在的文件块
	sparseIndex *block.SparseIndex

	// totalMap：全部 SSTable 文件路径记录（内存+磁盘）
	totalMap map[int][]FileInfo

	// 异步合并等待（Level1 及以上）
	compactionCond  *sync.Cond // 条件变量，用于等待异步合并完成
	compacting      bool       // 标识是否正在异步合并
	compactingLevel int        // 正在合并的层级，0 表示 Level0，-1 表示无
}

func NewSSTableManager() *Manager {
	mgr := &Manager{
		metas:           make([]*SSTable, 0),
		totalMap:        make(map[int][]FileInfo),
		indexMap:        make(map[string]int),
		sparseIndex:     block.NewSparseIndex(),
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
	m.addTable(sst)

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
			m.addTable(table)
		}
	}
	m.sparseIndex.Sort() // 排序稀疏索引
	idGenerator.Add(maxID)
	return nil
}

// addTable 将新的 SSTable 元数据插入到内存中，保证 metas 按 sst.id 降序排序，
// 并只保留前 maxSSTableCount 个文件。
func (m *Manager) addTable(table *SSTable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 更新 totalMap：将文件信息加入指定层的记录
	m.totalMap[table.level] = append(m.totalMap[table.level], FileInfo{
		minKey:   table.IndexBlock.StartKey,
		filePath: table.filePath,
	})

	idx := sort.Search(len(m.metas), func(i int) bool {
		if m.metas[i].level > table.level {
			return true
		}
		if m.metas[i].level < table.level {
			return false
		}
		return m.metas[i].id < table.id
	})

	// 若当前层为 Level1 及以上，则追加索引记录
	if table.level > minSSTableLevel {
		m.sparseIndex.AddFromIndexBlock(table.level, table.filePath, table.IndexBlock)
	}

	// 插入 metas，保证按照 sst.id 降序排序
	m.metas = append(m.metas, nil)
	copy(m.metas[idx+1:], m.metas[idx:])
	m.metas[idx] = table

	// 更新 indexMap，确保每个文件对应的索引正确
	for path, i := range m.indexMap {
		if i >= idx {
			m.indexMap[path] = i + 1
		}
	}
	m.indexMap[table.FilePath()] = idx

	// 超限则移除最后一个
	if len(m.metas) > maxSSTableCount {
		removed := m.metas[len(m.metas)-1]
		delete(m.indexMap, removed.FilePath())
		m.metas = m.metas[:len(m.metas)-1]
		removed.Close()
	}
}

// removeOldSSTables 用于删除指定旧文件的内存和磁盘信息
func (m *Manager) removeOldSSTables(oldFiles []string, level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, oldPath := range oldFiles {
		// 删除内存中的 SSTable 元数据（更新 metas 和 indexMap）
		if idx, ok := m.indexMap[oldPath]; ok && idx < len(m.metas) {
			m.metas = append(m.metas[:idx], m.metas[idx+1:]...)
			delete(m.indexMap, oldPath)
			// 调整 indexMap 中后续记录的索引
			for path, i := range m.indexMap {
				if i > idx {
					m.indexMap[path] = i - 1
				}
			}
		}

		// 从 totalMap 删除对应的 FileInfo
		filtered := make([]FileInfo, 0, len(m.totalMap[level]))
		for _, fi := range m.totalMap[level] {
			if fi.filePath != oldPath {
				filtered = append(filtered, fi)
			}
		}
		m.totalMap[level] = filtered

		// 更新稀疏索引：删除该文件对应的所有索引记录
		if level > minSSTableLevel {
			m.sparseIndex.RemoveByFileName(oldPath)
		}

		// 删除物理文件
		if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
			log.Errorf("remove file %s error: %s", oldPath, err.Error())
			return err
		}
	}

	return nil
}

// addNewSSTables 用于将新表加入内存和对应 level
func (m *Manager) addNewSSTables(newTables []*SSTable, level int) error {
	// 新表先全部写入磁盘，并用 addTable 增加内存记录，同时更新稀疏索引（追加新索引记录）
	for _, nt := range newTables {
		// 将 SSTable 写入磁盘
		if err := nt.EncodeTo(nt.FilePath()); err != nil {
			log.Errorf("encode sstable to file %s error: %s", nt.FilePath(), err.Error())
			return err
		}
		nt.DataBlocks = nil
		m.addTable(nt)
	}

	// 加锁后对稀疏索引整体排序，确保索引 Position 顺序正确
	m.mu.Lock()
	m.sparseIndex.Sort()
	m.mu.Unlock()

	return nil
}

// isFileInMemoryAndReturn 判断并返回内存中对应的 SSTable
func (m *Manager) isFileInMemoryAndReturn(filePath string) (*SSTable, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx, ok := m.indexMap[filePath]
	if !ok || idx >= len(m.metas) {
		return nil, false
	}
	return m.metas[idx], true
}

func (m *Manager) getFilesByLevel(level int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var files []string
	fileInfos := m.totalMap[level]
	for _, fileInfo := range fileInfos {
		files = append(files, fileInfo.filePath)
	}

	return files
}

func (m *Manager) isLevelNeedToBeMerged(level int) bool {
	return len(m.getFilesByLevel(level)) > maxFileNumsInLevel(level)
}

// getLevelSortedFilesByKey 返回磁盘中某层的所有文件，按照 minKey 进行排序
func (m *Manager) getLevelSortedFilesByKey(level int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var files []string
	fileInfos := m.totalMap[level]
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].minKey < fileInfos[j].minKey
	})
	for _, fileInfo := range fileInfos {
		files = append(files, fileInfo.filePath)
	}

	return files
}

// getSortedFilesByLevel 返回磁盘中某层的所有文件，按照 id（时间戳）的大小排序
func (m *Manager) getSortedFilesByLevel(level int) []string {
	files := m.getFilesByLevel(level)
	sort.Slice(files, func(i, j int) bool {
		return util.ExtractID(files[i]) < util.ExtractID(files[j])
	})

	return files
}

func (m *Manager) getLevelSmallestKFilesByKey(level int, k int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	fileInfos := m.totalMap[level]
	n := len(fileInfos)
	if k >= n {
		// 如果 k >= 全部文件数，直接全部排序返回
		sort.Slice(fileInfos, func(i, j int) bool {
			return fileInfos[i].minKey < fileInfos[j].minKey
		})
		files := make([]string, 0, n)
		for _, f := range fileInfos {
			files = append(files, f.filePath)
		}
		return files
	}

	// 使用 QuickSelect 找到前 k 小
	quickSelect(fileInfos, 0, n-1, k)

	// 对前 k 个元素再排序一次，确保顺序性
	sort.Slice(fileInfos[:k], func(i, j int) bool {
		return fileInfos[i].minKey < fileInfos[j].minKey
	})

	files := make([]string, 0, k)
	for i := 0; i < k; i++ {
		files = append(files, fileInfos[i].filePath)
	}
	return files
}

func (m *Manager) getSSTablesSortedByID(level int) []*SSTable {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []*SSTable
	files := m.totalMap[level]
	sort.Slice(files, func(i, j int) bool {
		return util.ExtractID(files[i].filePath) < util.ExtractID(files[j].filePath)
	})

	for _, fi := range files {
		if idx, ok := m.indexMap[fi.filePath]; ok && idx < len(m.metas) {
			result = append(result, m.metas[idx])
		}
	}
	return result
}

func (m *Manager) findPositionByKey(key kv.Key) *block.Position {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.sparseIndex.FindPositionByKey(key)
}

func (m *Manager) waitIfLevelCompacting(level int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	var wait bool
	for m.compacting && m.compactingLevel == level {
		wait = true
		m.compactionCond.Wait()
	}
	return wait
}

func (m *Manager) getCompactingLevel() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.compactingLevel
}

func quickSelect(arr []FileInfo, left, right, k int) {
	if left >= right {
		return
	}
	pivotIndex := partition(arr, left, right)
	if pivotIndex == k {
		return
	} else if pivotIndex > k {
		quickSelect(arr, left, pivotIndex-1, k)
	} else {
		quickSelect(arr, pivotIndex+1, right, k)
	}
}

func partition(arr []FileInfo, left, right int) int {
	pivot := arr[right]
	i := left
	for j := left; j < right; j++ {
		if arr[j].minKey < pivot.minKey {
			arr[i], arr[j] = arr[j], arr[i]
			i++
		}
	}
	arr[i], arr[right] = arr[right], arr[i]
	return i
}

func maxFileNumsInLevel(level int) int {
	return int(math.Pow(float64(levelSizeBase), float64(level+2)))
}
