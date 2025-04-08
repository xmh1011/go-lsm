package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/memtable"
)

func TestSSTableBuilderBasic(t *testing.T) {
	// 1. 构造一个 IMemtable，放几个键值对
	mem := memtable.NewMemtable(1, "")
	assert.NoError(t, mem.Insert(kv.KeyValuePair{Key: "alpha", Value: []byte("A")}))
	assert.NoError(t, mem.Insert(kv.KeyValuePair{Key: "beta", Value: []byte("B")}))
	assert.NoError(t, mem.Insert(kv.KeyValuePair{Key: "gamma", Value: []byte("G")}))

	imem := memtable.NewIMemtable(mem)

	// 2. 调用 BuildSSTableFromIMemtable
	sst := BuildSSTableFromIMemtable(imem)

	// 3. 验证 DataBlocks
	assert.NotNil(t, sst)
	assert.True(t, len(sst.DataBlocks) > 0, "should have at least one data block")

	// 4. 检查第一个 DataBlock 是否包含正确的记录
	firstBlock := sst.DataBlocks[0]
	assert.Equal(t, 3, len(firstBlock.Records), "data block should hold 3 pairs")

	assert.Equal(t, "alpha", string(firstBlock.Records[0].Key))
	assert.Equal(t, kv.Value("A"), firstBlock.Records[0].Value)
	assert.Equal(t, "beta", string(firstBlock.Records[1].Key))
	assert.Equal(t, kv.Value("B"), firstBlock.Records[1].Value)
	assert.Equal(t, "gamma", string(firstBlock.Records[2].Key))
	assert.Equal(t, kv.Value("G"), firstBlock.Records[2].Value)

	// 5. 验证 FilterBlock 是否已添加了这 3 个 key
	assert.NotNil(t, sst.FilterBlock)
	assert.True(t, sst.MayContain("alpha"))
	assert.True(t, sst.MayContain("beta"))
	assert.True(t, sst.MayContain("gamma"))
	assert.False(t, sst.MayContain("delta")) // 不存在

	// 6. 验证 IndexBlock
	assert.NotNil(t, sst.IndexBlock)
	assert.Equal(t, 2, len(sst.IndexBlock.Indexes), "only 1 block, so 1 index entry")

	assert.Equal(t, kv.Key("alpha"), sst.IndexBlock.Indexes[0].SeparatorKey, "start key of block is alpha")
	assert.Equal(t, kv.Key("gamma"), sst.IndexBlock.Indexes[len(sst.IndexBlock.Indexes)-1].SeparatorKey, "end key of block is gamma")
}

func TestSSTableBuilderShouldFlush(t *testing.T) {
	builder := NewSSTableBuilder(minSSTableLevel)
	// 在默认实现中, ShouldFlush() 判断 builder.size >= maxSSTableSize
	// 测试中可模拟 maxSSTableSize = 1024 or 2MB

	// 构造一个假 large pair
	largePair := &kv.KeyValuePair{
		Key:   "big-key",
		Value: make([]byte, 2*1024*1024), // 2MB
	}

	builder.Add(largePair)
	assert.True(t, builder.ShouldFlush())
}
