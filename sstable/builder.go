package sstable

import (
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/memtable"
	"github.com/xmh1011/go-lsm/sstable/block"
)

type Builder struct {
	table *SSTable
	size  uint64
}

func NewSSTableBuilder(level int) *Builder {
	return &Builder{
		table: NewSSTableWithLevel(level),
		size:  0,
	}
}

// BuildSSTableFromIMemtable 构建一个完整的 SSTable（包含 DataBlock、IndexBlock、FilterBlock）
func BuildSSTableFromIMemtable(imem *memtable.IMemtable) *SSTable {
	builder := NewSSTableBuilder(minSSTableLevel)

	// 遍历所有 key-value 对
	imem.RangeScan(func(pair *kv.KeyValuePair) {
		builder.Add(pair)
		builder.table.FilterBlock.Filter.Add([]byte(pair.Key))
	})

	return builder.Build()
}

// Add 向当前 DataBlock 添加记录；若当前 Block 满了，则 flush。
func (b *Builder) Add(pair *kv.KeyValuePair) {
	// 如果当前 DataBlock 不存在或已满，新建一个
	var current *block.DataBlock
	if len(b.table.DataBlocks) == 0 || !b.table.DataBlocks[len(b.table.DataBlocks)-1].CanInsert(pair) {
		current = block.NewDataBlock()
		b.table.DataBlocks = append(b.table.DataBlocks, current)
	} else {
		current = b.table.DataBlocks[len(b.table.DataBlocks)-1]
	}

	current.Records = append(current.Records, pair)
	b.table.FilterBlock.Add(pair.Key)
	b.size += pair.EstimateSize()
}

// ShouldFlush 判断是否应该写入磁盘
func (b *Builder) ShouldFlush() bool {
	return b.size >= maxSSTableSize
}

// Finalize 填充 FilterBlock 和 IndexBlock
func (b *Builder) Finalize() {
	for i, blk := range b.table.DataBlocks {
		if len(blk.Records) == 0 {
			continue
		}
		if i == 0 {
			entry := block.NewIndexEntry()
			entry.SeparatorKey = blk.Records[0].Key
			b.table.IndexBlock.Indexes = append(b.table.IndexBlock.Indexes, entry)
		}

		// 构建 IndexEntry
		entry := block.NewIndexEntry()
		entry.SeparatorKey = blk.Records[len(blk.Records)-1].Key
		// Handle 由 EncodeTo 时填充
		b.table.IndexBlock.Indexes = append(b.table.IndexBlock.Indexes, entry)
	}
}

// Build 返回最终构建好的 SSTable
func (b *Builder) Build() *SSTable {
	b.Finalize()
	return b.table
}
