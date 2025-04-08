package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
)

// TestCompactAndMergeBlocks_Basic 测试多个 DataBlock 经堆归并后是否生成预期的 SSTable。
func TestCompactAndMergeBlocksBasic(t *testing.T) {
	// 构造第1个数据块 block1
	block1 := block.NewDataBlock()
	block1.Records = []*kv.KeyValuePair{
		{Key: "alpha", Value: []byte("A")},
		{Key: "beta", Value: []byte("B")},
		{Key: "deletedKey", Value: nil, Deleted: true}, // 标记删除
	}

	// 构造第2个数据块 block2
	block2 := block.NewDataBlock()
	block2.Records = []*kv.KeyValuePair{
		{Key: "carrot", Value: []byte("C")},
		{Key: "delta", Value: []byte("D")},
	}

	// 执行归并，假设目标写到level=1
	blocks := []*block.DataBlock{block1, block2}
	results := CompactAndMergeBlocks(blocks, 1)

	// 只返回 1 个 SSTable（因为合并通常输出一个或少量SSTable，视大小而定）
	assert.Len(t, results, 1, "Expected exactly 1 sstable after merge")
	sst := results[0]
	assert.Equal(t, 1, sst.level, "sstable level should be 1")

	// 验证 sst.DataBlocks 是否有至少1个 block
	assert.NotEmpty(t, sst.DataBlocks, "Merged SSTable should have at least one DataBlock")

	// 因为没有限制 block 尺寸，全部记录都合并到一个 DataBlock
	mergedBlock := sst.DataBlocks[0]
	assert.NotNil(t, mergedBlock)
	assert.True(t, len(mergedBlock.Records) >= 4, "Expected at least 4 records (minus the deletedKey)")

	// 验证记录顺序 & 跳过已删除
	// merged 记录应是 alpha, beta, carrot, delta (都按照key字典序)
	var mergedKeys []string
	for _, pair := range mergedBlock.Records {
		mergedKeys = append(mergedKeys, string(pair.Key))
	}

	// deletedKey 应该被跳过
	assert.Equal(t, []string{"alpha", "beta", "carrot", "delta"}, mergedKeys, "deletedKey should not appear")

	// 验证 FilterBlock 内布隆过滤器（只要结果Builder里写入Add(key)即可）
	assert.True(t, sst.MayContain("alpha"))
	assert.True(t, sst.MayContain("delta"))
	assert.False(t, sst.MayContain("deletedKey"))

	// 验证 IndexBlock
	assert.NotEmpty(t, sst.IndexBlock.Indexes)
	assert.Equal(t, sst.IndexBlock.Indexes[0].SeparatorKey, kv.Key("alpha"))
	assert.Equal(t, sst.IndexBlock.Indexes[len(sst.IndexBlock.Indexes)-1].SeparatorKey, kv.Key("delta"))
}
