package sstable

import (
	"container/heap"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
)

// Iterator 用于遍历 SSTable 中所有记录（跨多个 DataBlock）
type Iterator struct {
	table    *SSTable
	blockIdx int                 // 当前 DataBlock 下标，对应 IndexBlock.Indexes
	dataIter *block.DataIterator // 当前 DataBlock 的迭代器
}

// NewSSTableIterator 创建 Iterator，并定位到第一个有效记录。
func NewSSTableIterator(t *SSTable) *Iterator {
	it := &Iterator{
		table:    t,
		blockIdx: -1,
	}
	it.SeekToFirst()
	return it
}

// loadCurrentBlock 根据 blockIdx 加载对应的 DataBlock，并创建 dataIter
func (it *Iterator) loadCurrentBlock() {
	dataBlock := it.table.LoadSpecifiedDataBlock(it.blockIdx)
	if dataBlock != nil {
		it.dataIter = block.NewDataBlockIterator(dataBlock)
	} else {
		it.dataIter = nil
	}
}

// Valid 判断当前迭代器是否处于有效位置
func (it *Iterator) Valid() bool {
	return it.dataIter != nil && it.dataIter.Valid()
}

// Key 返回当前记录的 key
func (it *Iterator) Key() kv.Key {
	if it.Valid() {
		return it.dataIter.Key()
	}
	return ""
}

// Value 返回当前记录的 value
func (it *Iterator) Value() kv.Value {
	if it.Valid() {
		return it.dataIter.Value()
	}
	return nil
}

// Pair 返回当前记录的 key-value 对
func (it *Iterator) Pair() *kv.KeyValuePair {
	if it.Valid() {
		return it.dataIter.Pair()
	}
	return nil
}

// Next 移动到下一个记录；如果当前 DataBlock 遍历完毕，则加载下一个 DataBlock
func (it *Iterator) Next() {
	if it.dataIter == nil {
		return
	}
	it.dataIter.Next()
	if !it.dataIter.Valid() {
		it.blockIdx++
		if it.blockIdx < len(it.table.IndexBlock.Indexes) {
			it.loadCurrentBlock()
			if it.dataIter != nil {
				it.dataIter.SeekToFirst()
			}
		} else {
			it.dataIter = nil
		}
	}
}

// SeekToFirst 定位到第一个记录
func (it *Iterator) SeekToFirst() {
	it.blockIdx = 0
	it.loadCurrentBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToFirst()
	}
}

// SeekToLast 定位到最后一个记录
func (it *Iterator) SeekToLast() {
	it.blockIdx = len(it.table.IndexBlock.Indexes) - 1
	it.loadCurrentBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToLast()
	}
}

// Seek 定位到第一个 key ≥ 指定 key 的记录
// 实现步骤：先在 IndexBlock 中二分查找目标 key 所在的 DataBlock 下标，再加载该 DataBlock 并在其中查找 key
func (it *Iterator) Seek(key kv.Key) {
	idx := it.findDataBlockIndex(key)
	if idx >= len(it.table.IndexBlock.Indexes) {
		it.dataIter = nil
		it.blockIdx = idx
		return
	}
	it.blockIdx = idx
	it.loadCurrentBlock()
	if it.dataIter != nil {
		it.dataIter.Seek(key)
		if !it.dataIter.Valid() {
			it.Next()
		}
	}
}

// findDataBlockIndex 使用二分查找在 IndexBlock 中确定 key 应该落在哪个 DataBlock（第一个 SeparatorKey ≥ key）
func (it *Iterator) findDataBlockIndex(key kv.Key) int {
	indexes := it.table.IndexBlock.Indexes
	lo, hi := 0, len(indexes)
	for lo < hi {
		mid := (lo + hi) / 2
		if indexes[mid].SeparatorKey < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Close 释放内部资源
func (it *Iterator) Close() {
	if it.dataIter != nil {
		it.dataIter.Close()
		it.dataIter = nil
	}
}

type MergeIterator struct {
	iter *block.DataIterator
	pair *kv.KeyValuePair
}

type minHeap []*MergeIterator

func (h *minHeap) Len() int {
	return len(*h)
}

func (h *minHeap) Less(i, j int) bool {
	return (*h)[i].pair.Key < (*h)[j].pair.Key
}

func (h *minHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *minHeap) Push(x any) {
	*h = append(*h, x.(*MergeIterator))
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// CompactAndMergeBlocks 直接根据排好序的 DataBlock 进行归并排序，提升性能
func CompactAndMergeBlocks(blocks []*block.DataBlock, level int) []*SSTable {
	h := &minHeap{}
	heap.Init(h)

	// 将每个 block 构建成迭代器
	for _, blk := range blocks {
		it := block.NewDataBlockIterator(blk)
		it.SeekToFirst()
		if it.Valid() {
			heap.Push(h, &MergeIterator{
				iter: it,
				pair: it.Pair(),
			})
		}
	}

	results := make([]*SSTable, 0)
	builder := NewSSTableBuilder(level)

	for h.Len() > 0 {
		item := heap.Pop(h).(*MergeIterator)

		if !item.pair.Deleted {
			builder.Add(item.pair)
		}
		item.iter.Next()
		if item.iter.Valid() {
			item.pair = item.iter.Pair()
			heap.Push(h, item)
		}
		if builder.ShouldFlush() {
			results = append(results, builder.Build())
			builder = NewSSTableBuilder(level)
		}
	}
	if builder.size > 0 {
		builder.table.level = level
		results = append(results, builder.Build())
	}

	return results
}
