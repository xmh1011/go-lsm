package skiplist

import (
	"github.com/xmh1011/go-lsm/kv"
)

// Iterator 是对跳表的只读迭代器封装
// 用于扫描等操作
// Iterator is a read-only iterator for the SkipList,
// used for range scans, flush operations, and compaction merges.
type Iterator struct {
	head *Node // Reference to the skiplist's head node.
	curr *Node // Current node the iterator is pointing to.
}

// NewSkipListIterator 返回一个从头开始遍历的跳表迭代器
// 为什么需要实现一个跳表迭代器呢？
// 1. 支持范围查询。跳表是有序结构，而迭代器可以实现从任意位置向后顺序遍历所有元素
// 2. Flush 到磁盘（SSTable 写入）。当 MemTable 要 flush 成 SSTable 时，需要全量有序写出所有键值对。
// 3. Compaction 合并多个层级数据。多个表（MemTable + SSTable）合并时，需要顺序遍历。
func NewSkipListIterator(s *SkipList) *Iterator {
	return &Iterator{
		head: s.Head,
		curr: s.Head.Forward[0],
	}
}

// SeekToFirst moves the iterator to the first valid node in the SkipList (skipping logically deleted nodes).
func (i *Iterator) SeekToFirst() {
	i.curr = i.head.Forward[0]
	// Skip over nodes marked as deleted.
	for i.curr != nil && i.curr.Pair.Deleted {
		i.curr = i.curr.Forward[0]
	}
}

// SeekToLast moves the iterator to the last valid node in the SkipList (skipping logically deleted nodes).
func (i *Iterator) SeekToLast() {
	i.curr = i.head
	// 从头开始顺序到达最后一个节点
	for i.curr.Forward[0] != nil {
		i.curr = i.curr.Forward[0]
	}
	// 如果最后节点被删除，则无法向后查找，可设为 nil
	if i.curr != nil && i.curr.Pair.Deleted {
		i.curr = nil
	}
}

// Seek 将迭代器定位到第一个 key 大于或等于指定值的有效节点。
func (i *Iterator) Seek(key kv.Key) {
	node := i.head
	// 从顶层向下逐层查找
	for level := maxLevel - 1; level >= 0; level-- {
		for node.Forward[level] != nil && node.Forward[level].Pair.Key < key {
			node = node.Forward[level]
		}
	}
	// 目标位置可能就是 node.Forward[0]
	node = node.Forward[0]

	// 跳过逻辑删除节点
	for node != nil && node.Pair.Deleted {
		node = node.Forward[0]
	}
	i.curr = node
}

// Valid returns true if the iterator points to a valid (non-deleted) node.
func (i *Iterator) Valid() bool {
	return i.curr != nil && !i.curr.Pair.Deleted
}

// Next moves the iterator to the next node in the SkipList.
func (i *Iterator) Next() {
	if i.curr != nil {
		i.curr = i.curr.Forward[0]
	}
}

// Key returns the key of the current node.
func (i *Iterator) Key() kv.Key {
	if i.curr != nil {
		return i.curr.Pair.Key
	}
	return ""
}

// Value returns the value of the current node.
func (i *Iterator) Value() kv.Value {
	if i.curr != nil {
		return i.curr.Pair.Value
	}
	return nil
}

// Pair returns the KeyValuePair of the current node.
func (i *Iterator) Pair() *kv.KeyValuePair {
	if i.curr != nil {
		return &i.curr.Pair
	}
	return nil
}

// Close closes the iterator (a no-op in Go, provided for interface completeness).
func (i *Iterator) Close() {
	i.curr = nil
}
