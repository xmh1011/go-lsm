package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
)

// createTestSSTable 构造一个 SSTable 实例用于测试。
// 该 SSTable 包含两个 DataBlock，每个 DataBlock 中包含若干 kv 对；
// 同时构造简单的 FilterBlock 和 Footer（假设 Footer 的 EncodeTo/DecodeFrom 已实现）。
func createTestSSTable() *SSTable {
	sst := NewSSTable()
	// 构造 DataBlock 1：记录 "a", "b"
	db1 := &block.DataBlock{
		Records: []*kv.KeyValuePair{
			{Key: "a", Value: []byte("A")},
			{Key: "b", Value: []byte("B")},
		},
	}
	sst.DataBlocks = append(sst.DataBlocks, db1)
	// 构造 DataBlock 2：记录 "c", "d"
	db2 := &block.DataBlock{
		Records: []*kv.KeyValuePair{
			{Key: "c", Value: []byte("C")},
			{Key: "d", Value: []byte("D")},
		},
	}
	sst.DataBlocks = append(sst.DataBlocks, db2)
	// 构造一个简单的 bloom filter，加入所有键
	sst.FilterBlock = block.NewFilterBlock(1024, 5)
	sst.FilterBlock.Filter.AddString("a")
	sst.FilterBlock.Filter.AddString("b")
	sst.FilterBlock.Filter.AddString("c")
	sst.FilterBlock.Filter.AddString("d")

	// 此处 IndexBlock 会在 EncodeTo 时被自动构造，故不用提前赋值
	return sst
}

func TestSSTableIteratorTraversalAndSeek(t *testing.T) {
	tempDir := t.TempDir()
	// 文件名采用 "1.sst"（ExtractIDFromFileName 会从文件名中提取 id）
	filePath := filepath.Join(tempDir, "1.sst")

	// 创建 SSTable 并写入文件
	original := createTestSSTable()
	err := original.EncodeTo(filePath)
	assert.NoError(t, err)

	// 从文件中加载 SSTable
	loaded := NewSSTable()
	err = loaded.DecodeFrom(filePath)
	assert.NoError(t, err)

	original.file, err = os.Open(filePath)
	assert.NoError(t, err)

	// 1. 顺序遍历 SSTable 中所有记录
	iter := NewSSTableIterator(original)
	var keys []string
	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		iter.Next()
	}
	// 预期记录顺序为："a", "b", "c", "d"
	assert.Equal(t, []string{"a", "b", "c", "d"}, keys)
	iter.Close()

	// 2. 测试 Seek 定位
	// (a) Seek 到 "b"，预期定位到 "b"
	loaded.file, err = os.Open(filePath)
	assert.NoError(t, err)
	iter = NewSSTableIterator(loaded)
	iter.Seek("b")
	assert.True(t, iter.Valid())
	assert.Equal(t, "b", string(iter.Key()))
	// Next 后应为 "c"
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, "c", string(iter.Key()))
	iter.Close()

	// (b) Seek 到 "bb"（"bb"不存在，但位于 "b" 与 "c"之间），预期返回 "c"
	iter = NewSSTableIterator(loaded)
	iter.Seek("bb")
	assert.True(t, iter.Valid())
	assert.Equal(t, "c", string(iter.Key()))
	iter.Close()

	// (c) Seek 到一个大于所有 key 的值，如 "z"，应定位为无效
	iter = NewSSTableIterator(loaded)
	iter.Seek("z")
	assert.False(t, iter.Valid())
	iter.Close()
}
