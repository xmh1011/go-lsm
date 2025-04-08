package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
)

func TestSearchFromFile(t *testing.T) {
	// 1. 创建临时文件
	dir := t.TempDir()
	level := 1
	path := sstableFilePath(1, level, dir)

	// 2. 构建 SSTable 并写入数据
	sst := NewSSTable()
	sst.DataBlocks = []*block.DataBlock{
		{Records: []*kv.KeyValuePair{
			{Key: "a", Value: []byte("apple")},
			{Key: "b", Value: []byte("banana")},
			{Key: "c", Value: []byte("cherry")},
			{Key: "d", Value: []byte("durian")},
		}},
	}

	// 3. 初始化 FilterBlock 并填充 key
	sst.FilterBlock = block.NewFilterBlock(1024, 5)
	for _, blk := range sst.DataBlocks {
		for _, pair := range blk.Records {
			sst.FilterBlock.Filter.Add([]byte(pair.Key))
		}
	}

	// 4. 写入文件
	err := sst.EncodeTo(path)
	assert.NoError(t, err)

	// 5. 确认文件存在
	assert.FileExists(t, path)

	// 6. SearchFromFile 测试
	val, err := SearchFromFile(sstableFilePath(1, level, dir), "b")
	assert.NoError(t, err)
	assert.Equal(t, []byte("banana"), val)

	val, err = SearchFromFile(sstableFilePath(1, level, dir), "a")
	assert.NoError(t, err)
	assert.Equal(t, []byte("apple"), val)

	val, err = SearchFromFile(sstableFilePath(1, level, dir), "d")
	assert.NoError(t, err)
	assert.Equal(t, []byte("durian"), val)

	// 7. 不存在的 key
	val, err = SearchFromFile(sstableFilePath(1, level, dir), "z")
	assert.NoError(t, err)
	assert.Nil(t, val)
}
