package sstable

import (
	"container/heap"

	"github.com/xmh1011/go-lsm/kv"
)

// KVEntry 包装 KV 对，用于堆排序
type KVEntry struct {
	pair kv.KeyValuePair
}

// minHeap 按 Key 排序的最小堆
type minHeap []*KVEntry

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
	*h = append(*h, x.(*KVEntry))
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// CompactAndMergeKVs 归并排序并去重（相同 Key 只保留最新的，假设输入中相同 Key 的最新 KV 对在前）
func CompactAndMergeKVs(kvs []kv.KeyValuePair, level int) []*SSTable {
	h := &minHeap{}
	heap.Init(h)

	// 1. 收集所有 KV 对并初始化堆
	for _, pair := range kvs {
		heap.Push(h, &KVEntry{pair: pair})
	}

	results := make([]*SSTable, 0)
	builder := NewSSTableBuilder(level)

	var lastWrittenKey kv.Key // 记录上一个写入的 Key

	// 2. 归并排序并去重
	for h.Len() > 0 {
		// 获取当前最小的 Key（堆顶）
		top := (*h)[0]
		currentKey := top.pair.Key

		// 如果当前 Key 和上一个写入的 Key 相同，跳过（去重）
		if len(lastWrittenKey) > 0 && currentKey == lastWrittenKey {
			heap.Pop(h) // 直接弹出，不处理
			continue
		}

		// 弹出堆顶元素（此时一定是当前最小的、未处理的 Key）
		entry := heap.Pop(h).(*KVEntry)
		currentPair := entry.pair

		// 如果不是删除标记，则写入
		// TODO: 最后一层合并时，将删除标记的 Key 写入 tombstone 文件
		if !currentPair.IsDeleted() || level < maxSSTableLevel {
			builder.Add(&currentPair)
			lastWrittenKey = currentKey // 更新最后写入的 Key
		}

		// 检查是否需要 Flush
		if builder.ShouldFlush() {
			results = append(results, builder.Build())
			builder = NewSSTableBuilder(level)
			lastWrittenKey = "" // 重置 lastWrittenKey
		}
	}

	// 3. 处理剩余数据
	if builder.size > 0 {
		builder.table.level = level
		results = append(results, builder.Build())
	}

	return results
}
