package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
	"github.com/xmh1011/go-lsm/sstable/bloom"
)

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
		sst.DataBlock.Add(record.Value)
		sst.FilterBlock.Add([]byte(record.Key))
		sst.IndexBlock.Add(record.Key, 0)
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
	dir := config.GetSSTablePath()
	sst := NewSSTable()
	sst.id = 1
	sst.level = 0
	sst.filePath = sstableFilePath(sst.id, sst.level, dir)

	// 设置测试数据
	record := kv.KeyValuePair{
		Key:   "key1",
		Value: []byte("value1"),
	}
	sst.FilterBlock = bloom.NewBloomFilter(1024, 3)
	sst.DataBlock.Add(record.Value)
	sst.FilterBlock.Add([]byte(record.Key))
	sst.IndexBlock.Add(record.Key, 0)
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
