package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/memtable"
	"github.com/xmh1011/go-lsm/sstable/block"
	"github.com/xmh1011/go-lsm/sstable/bloom"
)

func TestSSTableManagerCreateNewSSTable(t *testing.T) {
	tmp := t.TempDir()

	// 初始化可变 MemTable 并写入数据
	mem := memtable.NewMemTable(1, tmp)
	_ = mem.Insert(kv.KeyValuePair{Key: "key1", Value: []byte("value1")})
	_ = mem.Insert(kv.KeyValuePair{Key: "key2", Value: []byte("value2")})

	// 冻结为 IMemTable
	imem := memtable.NewIMemTable(mem)
	// 构建 Manager
	ResetIDGenerator()
	manager := NewSSTableManager()

	// 调用 CreateNewSSTable
	err := manager.CreateNewSSTable(imem)
	assert.NoError(t, err)

	// 检查文件是否存在
	files := manager.getFilesByLevel(0)
	assert.Len(t, files, 1)
	assert.FileExists(t, files[0])

	// 检查是否添加到内存索引
	tables := manager.getLevelTables(0)
	assert.Len(t, tables, 1)
	assert.Equal(t, uint64(1), tables[0].id)

	// 检查 WAL 文件是否被 Clean 删除
	_, err = os.Stat(filepath.Join(tmp, fmt.Sprintf("%d.wal", mem.ID())))
	assert.True(t, os.IsNotExist(err), "WAL 文件应被 Clean 删除")
}

func TestSSTableManagerSearch(t *testing.T) {
	// 1. 创建临时目录和测试数据
	dir := t.TempDir()

	// 2. 创建并初始化SSTable
	sst := NewSSTable()
	sst.id = 1
	sst.level = 0
	sst.filePath = sstableFilePath(sst.id, sst.level, dir)

	// 设置测试数据
	testRecords := []*kv.KeyValuePair{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
	}

	// 创建数据块和过滤器
	sst.FilterBlock = bloom.NewBloomFilter(1024, 3)
	for _, record := range testRecords {
		sst.Add(record)
	}
	sst.Header = block.NewHeader(testRecords[0].Key, testRecords[len(testRecords)-1].Key)

	// 3. 编码并写入文件
	err := sst.EncodeTo(sst.filePath)
	assert.NoError(t, err)
	assert.FileExists(t, sst.filePath)

	// 4. 初始化SSTableManager
	manager := NewSSTableManager()
	err = manager.addNewSSTables([]*SSTable{sst})
	assert.NoError(t, err)

	// 5. 测试Search功能
	tests := []struct {
		name     string
		key      kv.Key
		expected []byte
		wantErr  bool
	}{
		{
			name:     "existing key in first block",
			key:      "key1",
			expected: []byte("value1"),
			wantErr:  false,
		},
		{
			name:     "existing key in middle",
			key:      "key2",
			expected: []byte("value2"),
			wantErr:  false,
		},
		{
			name:     "existing key in last position",
			key:      "key3",
			expected: []byte("value3"),
			wantErr:  false,
		},
		{
			name:     "non-existing key",
			key:      "key4",
			expected: nil,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := manager.Search(tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, val)
		})
	}

	// 6. 测试布隆过滤器优化 - 查询明显不存在的key
	val, err := manager.Search("definitely_not_exist_key")
	assert.NoError(t, err)
	assert.Nil(t, val)
}

func TestSSTableManagerRecover(t *testing.T) {
	tmp := t.TempDir()
	config.Conf.SSTablePath = tmp
	for level := 0; level <= maxSSTableLevel; level++ {
		dir := sstableLevelPath(level, tmp)
		err := os.MkdirAll(dir, 0755)
		assert.NoError(t, err, "Failed to create directory for level %d", level)
	}
	sst := NewSSTable()
	sst.id = 1
	sst.level = 0
	sst.filePath = sstableFilePath(sst.id, sst.level, tmp)

	// 设置测试数据
	record := kv.KeyValuePair{
		Key:   "key1",
		Value: []byte("value1"),
	}
	sst.FilterBlock = bloom.NewBloomFilter(1024, 3)
	sst.Add(&record)
	sst.Header = block.NewHeader(record.Key, record.Key)

	// 编码并写入文件
	err := sst.EncodeTo(sst.filePath)
	assert.NoError(t, err)
	assert.FileExists(t, sst.filePath)

	// 初始化 Manager 并恢复
	manager := NewSSTableManager()
	err = manager.Recover() // 从磁盘加载元数据
	assert.NoError(t, err)

	// 验证恢复结果
	tables := manager.getLevelTables(0)
	assert.Len(t, tables, 1)
	assert.Equal(t, uint64(1), tables[0].id)
}

func TestSSTableManagerRemoveOldSSTables(t *testing.T) {
	dir := t.TempDir()

	// 1. 创建旧 SSTable 文件
	sst1 := NewSSTable()
	sst1.id = 1
	sst1.level = 0
	sst1.filePath = filepath.Join(dir, "1.sst")
	err := os.WriteFile(sst1.filePath, []byte("dummy"), 0644)
	assert.NoError(t, err)

	// 2. 初始化 Manager 并删除旧文件
	manager := NewSSTableManager()
	manager.fileIndex[sst1.filePath] = sst1 // 手动注册到索引
	manager.totalMap[0] = []string{sst1.filePath}

	err = manager.removeOldSSTables([]string{sst1.filePath}, 0)
	assert.NoError(t, err)

	// 3. 验证文件已被删除
	_, err = os.Stat(sst1.filePath)
	assert.True(t, os.IsNotExist(err))
}

func TestSSTableManagerAddTableOrderingAndIndex(t *testing.T) {
	manager := NewSSTableManager()

	sst1 := NewSSTable()
	sst1.id = 2
	sst1.level = 1
	sst1.Header = block.NewHeader("a", "k")
	sst1.filePath = "mock/path/2.sst"

	sst2 := NewSSTable()
	sst2.id = 1
	sst2.level = 1
	sst2.Header = block.NewHeader("p", "z")
	sst2.filePath = "mock/path/1.sst"

	manager.addTable(sst1)
	manager.addTable(sst2)

	tables := manager.getLevelTables(1)
	assert.Equal(t, []uint64{1, 2}, []uint64{tables[0].id, tables[1].id}, "SSTable 应该按 id 降序插入")

	sparse := manager.sparseIndexes[0]
	assert.Equal(t, 2, len(sparse))
	assert.Equal(t, sst1.id, sparse[0].id)
}

func TestWaitForCompactionIfNeeded(t *testing.T) {
	manager := NewSSTableManager()
	level := 2

	// 模拟合并正在进行
	manager.compactingLevels[level] = true

	done := make(chan struct{})
	go func() {
		go func() {
			time.Sleep(100 * time.Millisecond)
			manager.mu.Lock()
			manager.compactingLevels[level] = false
			manager.compactionCond.Broadcast()
			manager.mu.Unlock()
		}()
		err := manager.waitForCompactionIfNeeded(level)
		assert.NoError(t, err)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("waitForCompactionIfNeeded 超时")
	}
}

func TestIsLevelNeedToBeMerged(t *testing.T) {
	manager := NewSSTableManager()

	// 模拟超过限制的文件
	level := 2
	numsInLevel := maxFileNumsInLevel(level)
	paths := make([]string, numsInLevel+1)
	for i := 0; i < numsInLevel+1; i++ {
		paths[i] = filepath.Join("mock", fmt.Sprintf("%d.sst", i))
	}
	manager.totalMap[level] = paths

	assert.True(t, manager.isLevelNeedToBeMerged(level))
}

func TestGetSSTableByPath(t *testing.T) {
	manager := NewSSTableManager()

	sst := NewSSTable()
	sst.id = 42
	sst.level = 0
	sst.filePath = "mock/path/42.sst"

	manager.addTable(sst)

	got, ok := manager.getSSTableByPath("mock/path/42.sst")
	assert.True(t, ok)
	assert.Equal(t, uint64(42), got.id)
}

func TestAddNewSSTablesFailToWrite(t *testing.T) {
	sst := NewSSTable()
	sst.id = 1
	sst.level = 0
	sst.filePath = "/invalid/path/1.sst" // 故意非法路径

	manager := NewSSTableManager()
	err := manager.addNewSSTables([]*SSTable{sst})
	assert.Error(t, err)
}

func TestRecoverMultipleLevels(t *testing.T) {
	tmp := t.TempDir()
	config.Conf.SSTablePath = tmp
	for level := 0; level <= maxSSTableLevel; level++ {
		dir := sstableLevelPath(level, tmp)
		err := os.MkdirAll(dir, 0755)
		assert.NoError(t, err, "Failed to create directory for level %d", level)

		sst := NewSSTable()
		sst.id = uint64(level + 1)
		sst.level = level
		sst.filePath = filepath.Join(dir, fmt.Sprintf("%d.sst", sst.id))
		sst.Header = block.NewHeader("a", "z")
		sst.FilterBlock = bloom.NewBloomFilter(1024, 3)
		sst.Add(&kv.KeyValuePair{Key: "a", Value: []byte("x")})
		err = sst.EncodeTo(sst.filePath)
		assert.NoError(t, err)
	}

	manager := NewSSTableManager()
	err := manager.Recover()
	assert.NoError(t, err)

	for level := 0; level <= maxSSTableLevel; level++ {
		tables := manager.getLevelTables(level)
		assert.Len(t, tables, 1)
		assert.Equal(t, uint64(level+1), tables[0].id)
	}
}
