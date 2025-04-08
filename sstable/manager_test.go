package sstable

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
)

func TestSSTableManagerSearch(t *testing.T) {
	// 1. 临时目录
	dir := t.TempDir()

	// 2. 构造一个 SSTable 并写入数据
	sst := NewSSTable()
	sst.filePath = sstableFilePath(sst.id, sst.level, dir)
	sst.DataBlocks = []*block.DataBlock{
		{
			Records: []*kv.KeyValuePair{
				{Key: "a", Value: []byte("apple")},
				{Key: "b", Value: []byte("banana")},
				{Key: "c", Value: []byte("cherry")},
			},
		},
	}

	// 构建 FilterBlock
	sst.FilterBlock = block.NewFilterBlock(1024, 3)
	for _, blk := range sst.DataBlocks {
		for _, pair := range blk.Records {
			sst.FilterBlock.Filter.Add([]byte(pair.Key))
		}
	}

	// 3. 写入文件
	file := sst.FilePath()
	err := sst.EncodeTo(file)
	assert.NoError(t, err)
	assert.FileExists(t, file)

	// 4. 模拟从磁盘加载（未缓存状态）
	manager := NewSSTableManager()
	manager.DiskMap[0] = []string{file}

	// 5. 执行 Search，测试命中
	val, err := manager.Search("b")
	assert.NoError(t, err)
	assert.Equal(t, []byte("banana"), val)

	// 再次查询相同 key，应命中缓存
	val, err = manager.Search("a")
	assert.NoError(t, err)
	assert.Equal(t, []byte("apple"), val)

	// 查询不存在 key，应返回 err
	val, err = manager.Search("z")
	assert.NoError(t, err)
	assert.Nil(t, val)

	// 检查是否被添加到缓存中
	all := manager.getAll()
	assert.Len(t, all, 1)
	assert.Equal(t, sst.id, all[0].id)

	// 检查 map 中的记录是否存在
	assert.False(t, slices.Contains(manager.DiskMap[0], file))
}
