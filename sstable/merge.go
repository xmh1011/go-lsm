package sstable

import (
	"container/heap"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
)

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
