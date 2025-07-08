package sstable

import (
	"fmt"
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
		if err = os.MkdirAll(sstableLevelPath(i, config.GetSSTablePath()), os.ModePerm); err != nil {
			log.Errorf("failed to create sstable level %d directory: %s", i, err.Error())
		}
	}
}

// Manager 管理内存中的 SSTable 元信息（Footer/Filter/Index）+ 磁盘中剩余的文件记录。
type Manager struct {
	mu sync.RWMutex

	// levels 保存各层级的 SSTable 元信息，按层级分组，每层内按 id 降序排序
	levels [][]*SSTable

	// fileIndex 用于快速查找 SSTable 的索引 (文件路径 -> *SSTable)
	fileIndex map[string]*SSTable

	// totalMap 记录所有层级的文件路径
	totalMap map[int][]string

	// 异步合并控制
	compactionCond   *sync.Cond
	compactingLevels map[int]bool // 记录各层级的压缩状态
}

func NewSSTableManager() *Manager {
	mgr := &Manager{
		levels:           make([][]*SSTable, maxSSTableLevel+1),
		fileIndex:        make(map[string]*SSTable),
		totalMap:         make(map[int][]string),
		compactingLevels: make(map[int]bool),
	}
	mgr.compactionCond = sync.NewCond(&mgr.mu)
	return mgr
}

// CreateNewSSTable 将 imem 数据构建为 SSTable，写入到磁盘，然后将其元数据添加到内存中。
func (m *Manager) CreateNewSSTable(imem *memtable.IMemTable) error {
	sst := BuildSSTableFromIMemTable(imem)
	defer imem.Clean() // 删除 WAL 文件

	// 写入 Level0 文件
	filePath := sstableFilePath(sst.id, sst.level, config.GetSSTablePath())
	if err := sst.EncodeTo(filePath); err != nil {
		log.Errorf("encode sstable to file %s error: %s", sst.FilePath(), err.Error())
		return fmt.Errorf("encode sstable failed: %w", err)
	}

	// 添加到内存中
	m.addTable(sst)

	// 执行合并逻辑
	if err := m.Compaction(); err != nil {
		log.Errorf("compaction error: %s", err.Error())
		return fmt.Errorf("compaction failed: %w", err)
	}

	return nil
}

// Search 从低层级向高层级查找 key，同层级按 id 降序查找
// 返回找到的值或错误，如果未找到返回 (nil, nil)
func (m *Manager) Search(key kv.Key) ([]byte, error) {
	// 1. 从高层级向低层级查找
	for level := minSSTableLevel; level <= maxSSTableLevel; level++ {
		// 2. 等待该层级的潜在合并完成（仅对需要等待的层级）
		if err := m.waitForCompactionIfNeeded(level); err != nil {
			log.Errorf("wait for compaction at level %d failed: %s", level, err.Error())
			return nil, fmt.Errorf("wait for compaction failed: %w", err)
		}

		// 3. 获取该层级的表切片（已按ID降序排列）
		tables := m.getLevelTables(level)

		// 4. 在当前层级中按表ID降序查找
		for _, table := range tables {
			val, err := m.SearchFromTable(table, key)
			if err != nil {
				// 记录警告但继续查找其他表
				log.Warnf("search from table %s failed: %s", table.FilePath(), err.Error())
				continue
			}
			if val != nil {
				return val, nil
			}
		}
	}

	// 5. 所有层级都未找到
	return nil, nil
}

// waitForCompactionIfNeeded 等待指定层级完成合并（如果正在合并）
// 如果层级正在合并，则阻塞直到合并完成；否则立即返回
// 返回可能因等待被中断而产生的错误
func (m *Manager) waitForCompactionIfNeeded(level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 快速检查：如果该层级没有在合并，直接返回
	if !m.isCompacting(level) {
		return nil
	}

	// 等待合并完成
	for m.isCompacting(level) {
		m.compactionCond.Wait()
	}

	return nil
}

// isCompacting 辅助方法：检查指定层级是否正在合并
func (m *Manager) isCompacting(level int) bool {
	return m.compactingLevels[level]
}

func (m *Manager) SearchFromTable(sst *SSTable, key kv.Key) (kv.Value, error) {
	if !sst.MayContain(key) {
		return nil, nil
	}

	// 使用迭代器查找
	it := NewSSTableIterator(sst)
	defer it.Close()

	it.Seek(key)
	if it.Valid() && it.Key() == key {
		return it.Value()
	}
	return nil, nil
}

// Recover 加载所有层中 SSTable 的元数据信息到内存中
func (m *Manager) Recover() error {
	var maxID uint64

	for level := minSSTableLevel; level <= maxSSTableLevel; level++ {
		dir := sstableLevelPath(level, config.GetSSTablePath())
		files, err := os.ReadDir(dir)
		if err != nil {
			log.Errorf("failed to read directory %s: %s", dir, err.Error())
			return fmt.Errorf("read directory %s failed: %w", dir, err)
		}

		if len(files) == 0 {
			log.Debugf("directory %s is empty, skipping", dir)
			continue
		}

		// 按文件名降序排序（假设文件名包含ID）
		sort.Slice(files, func(i, j int) bool {
			return util.ExtractID(files[i].Name()) > util.ExtractID(files[j].Name())
		})

		// 记录最大ID
		latestID := util.ExtractID(files[0].Name())
		if latestID > maxID {
			maxID = latestID
		}

		// 加载每个文件的元数据
		for _, file := range files {
			if file.IsDir() {
				continue
			}

			filePath := filepath.Join(dir, file.Name())
			table := NewRecoverSSTable(level)
			table.id = util.ExtractID(file.Name())

			if err := table.DecodeFrom(filePath); err != nil {
				log.Errorf("recover: load meta for file %s error: %s", filePath, err.Error())
				return fmt.Errorf("load meta for file %s failed: %w", filePath, err)
			}

			m.addTable(table)
		}
	}

	idGenerator.Add(maxID)
	return nil
}

// addTable 将新的 SSTable 添加到内存中，保持层级和排序（新文件在最前面）
func (m *Manager) addTable(table *SSTable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	table.DataBlock = block.NewDataBlock()
	level := table.level
	tables := m.levels[level]

	// 直接插入到列表开头（保持降序）
	m.levels[level] = append([]*SSTable{table}, tables...)

	// 添加到文件索引
	m.fileIndex[table.FilePath()] = table

	m.totalMap[level] = append(m.totalMap[level], table.FilePath())
}

// removeOldSSTables 删除旧的 SSTable 文件
func (m *Manager) removeOldSSTables(oldFiles []string, level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, oldPath := range oldFiles {
		// 从层级中移除
		if sst, exists := m.fileIndex[oldPath]; exists {
			tables := m.levels[level]
			for i, t := range tables {
				if t.id == sst.id {
					m.levels[level] = append(tables[:i], tables[i+1:]...)
					break
				}
			}
		}
		// 从文件索引中移除
		delete(m.fileIndex, oldPath)

		// 从 totalMap 中移除
		m.totalMap[level] = util.RemoveString(m.totalMap[level], oldPath)

		// 物理删除文件
		if err := os.Remove(oldPath); err != nil {
			log.Errorf("remove file %s error: %s", oldPath, err.Error())
			return err
		}
	}

	return nil
}

// addNewSSTables 添加新的 SSTable 到指定层级
func (m *Manager) addNewSSTables(newTables []*SSTable) error {
	for _, nt := range newTables {
		// 写入磁盘
		if err := nt.EncodeTo(nt.FilePath()); err != nil {
			log.Errorf("encode sstable to file %s error: %s", nt.FilePath(), err.Error())
			return fmt.Errorf("encode sstable failed: %w", err)
		}

		// 添加到内存
		m.addTable(nt)
	}

	return nil
}

// getLevelTables 获取指定层级的所有 SSTable（已排序）
func (m *Manager) getLevelTables(level int) []*SSTable {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tables := m.levels[level]
	if tables == nil {
		return nil
	}

	// 返回副本以避免外部修改
	result := make([]*SSTable, len(tables))
	copy(result, tables)
	return result
}

// getFilesByLevel 获取指定层级的所有文件路径
func (m *Manager) getFilesByLevel(level int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return append([]string{}, m.totalMap[level]...)
}

// isLevelNeedToBeMerged 检查层级是否需要合并
func (m *Manager) isLevelNeedToBeMerged(level int) bool {
	return len(m.getFilesByLevel(level)) > maxFileNumsInLevel(level)
}

func maxFileNumsInLevel(level int) int {
	return int(math.Pow(float64(levelSizeBase), float64(level+1)))
}

func (m *Manager) getSSTableByPath(path string) (*SSTable, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sst, ok := m.fileIndex[path]
	return sst, ok
}
