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

// BuildSSTableFromIMemTable 构建一个完整的 SSTable（包含 DataBlock、IndexBlock、FilterBlock）
func BuildSSTableFromIMemTable(imem *memtable.IMemTable) *SSTable {
	builder := NewSSTableBuilder(minSSTableLevel)

	// 遍历所有 key-value 对
	imem.RangeScan(func(pair *kv.KeyValuePair) {
		builder.Add(pair)
	})

	return builder.Build()
}

// Add 向当前 DataBlock（[]kv.Value）添加记录；若当前 Block 满了，则创建新 Block。
func (b *Builder) Add(pair *kv.KeyValuePair) {
	b.table.Add(pair)
	b.size += pair.EstimateSize()
}

// ShouldFlush 判断是否应该写入磁盘
func (b *Builder) ShouldFlush() bool {
	return b.size >= maxSSTableSize
}

// Finalize 填充 IndexBlock 和 Header
func (b *Builder) Finalize() {
	// 初始化 Header
	if b.table.DataBlock.Len() > 0 {
		b.table.Header = &block.Header{
			MaxKey: b.table.IndexBlock.Indexes[b.table.DataBlock.Len()-1].Key,
			MinKey: b.table.IndexBlock.Indexes[0].Key,
		}
	}
}

// Build 返回最终构建好的 SSTable
func (b *Builder) Build() *SSTable {
	b.Finalize()
	return b.table
}
