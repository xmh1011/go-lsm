package sstable

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
	"github.com/xmh1011/go-lsm/sstable/bloom"
)

func TestSSTableEncodeDecode(t *testing.T) {
	// 创建临时目录和文件路径
	dir := t.TempDir()
	path := filepath.Join(dir, "1.sst")

	// 构造 DataBlock
	dataBlock := &block.DataBlock{
		Records: []*kv.KeyValuePair{
			{Key: "alpha", Value: []byte("A")},
			{Key: "beta", Value: []byte("B")},
		},
	}

	// 构造 FilterBlock
	filter := bloom.NewBloomFilter(1024, 5)
	filter.AddString("alpha")
	filter.AddString("beta")

	// 构造 SSTable
	original := &SSTable{
		DataBlocks: []*block.DataBlock{dataBlock},
		FilterBlock: &block.FilterBlock{
			Filter: filter,
		},
	}

	// 写入到文件
	err := original.EncodeTo(path)
	assert.NoError(t, err)

	// 读取回来
	loaded := NewSSTable()
	err = loaded.DecodeFrom(path)
	assert.NoError(t, err)

	// 验证数据块记录一致
	assert.Equal(t, len(original.DataBlocks), len(loaded.DataBlocks))
	assert.Equal(t, original.DataBlocks[0].Records, loaded.DataBlocks[0].Records)

	// 验证过滤器内容
	assert.True(t, loaded.FilterBlock.MayContain("alpha"))
	assert.True(t, loaded.FilterBlock.MayContain("beta"))
	assert.False(t, loaded.FilterBlock.MayContain("gamma"))

	// 验证 IndexBlock 长度一致
	assert.Equal(t, len(original.IndexBlock.Indexes), len(loaded.IndexBlock.Indexes))
}
