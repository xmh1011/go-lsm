package sstable

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
)

// TestSSTableManagerSearch 重写单元测试，确保恢复时将文件路径同时添加到 DiskMap 和 TotalMap，
// 并验证 Search 能正确返回预期值，同时 SSTable 被加载到内存缓存中。
func TestSSTableManagerSearch(t *testing.T) {
	// 1. 创建临时目录
	dir := t.TempDir()

	// 2. 构造一个 SSTable 并写入数据
	sst := NewSSTable()
	sst.id = 1
	// 设置文件路径，确保文件存储在临时目录下
	sst.filePath = sstableFilePath(sst.id, sst.level, dir)
	// 构造 DataBlocks，模拟 SSTable 中的数据
	sst.DataBlocks = []*block.DataBlock{
		{
			Records: []*kv.KeyValuePair{
				{Key: "a", Value: []byte("apple")},
				{Key: "b", Value: []byte("banana")},
				{Key: "c", Value: []byte("cherry")},
			},
		},
	}

	// 构建 FilterBlock，保证查询时布隆过滤器能正常判断
	sst.FilterBlock = block.NewFilterBlock(1024, 3)
	for _, blk := range sst.DataBlocks {
		for _, pair := range blk.Records {
			sst.FilterBlock.Filter.Add([]byte(pair.Key))
		}
	}

	// 3. 写入 SSTable 到文件系统
	file := sst.FilePath()
	err := sst.EncodeTo(file)
	assert.NoError(t, err)
	assert.FileExists(t, file)

	// 4. 创建一个 Manager 实例，并把生成的文件路径同时记录到 DiskMap 和 TotalMap 中，
	// 模拟从磁盘加载的情况（未缓存状态）。
	manager := NewSSTableManager()
	manager.DiskMap[0] = []string{file}
	manager.TotalMap[0] = []string{file}

	// 5. 执行 Search 测试：查询 key "b"，预期返回 "banana"
	val, err := manager.Search("b")
	assert.NoError(t, err)
	assert.Equal(t, []byte("banana"), val)

	// 6. 再次查询 key "a"，此时应命中缓存，预期返回 "apple"
	val, err = manager.Search("a")
	assert.NoError(t, err)
	assert.Equal(t, []byte("apple"), val)

	// 7. 查询不存在的 key "z"，预期返回 nil
	val, err = manager.Search("z")
	assert.NoError(t, err)
	assert.Nil(t, val)

	// 8. 检查内存缓存中是否存在已加载的 SSTable 元信息
	all := manager.getAll()
	assert.Len(t, all, 1)
	assert.Equal(t, sst.id, all[0].id)

	// 9. 检查 DiskMap 中是否已移除该文件（因为已加载到内存中）
	assert.False(t, slices.Contains(manager.DiskMap[0], file))
}
