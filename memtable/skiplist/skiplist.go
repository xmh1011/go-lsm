// This file is an implementation for skipList data structure.
// Reference: https://oi-wiki.org/ds/skiplist/
// SkipList is a probabilistic data structure that allows fast search, insertion, and deletion
// operations—similar to balanced trees, but easier to implement.
// It maintains elements in a sorted order and achieves efficiency by maintaining multiple levels of Forward pointers.
// At the base Level, a skip list behaves like a sorted linked list.
// On top of that, additional layers are added where each element appears with a certain probability (commonly 1/2 or 1/4).
// These upper layers serve as "express lanes" that allow the algorithm to skip over many elements during a search,
// reducing the average time complexity of search, insert, and delete operations to O(log n).

package skiplist

import (
	"math/rand/v2"

	"github.com/xmh1011/go-lsm/kv"
)

const (
	maxLevel = 32
	pFactor  = 0.25
)

// Node 跳表节点的实现
// Pair: 存储在kv包中定义的KV键值对
// 根据kv的key大小来排序存储数据
type Node struct {
	Pair    kv.KeyValuePair
	Forward []*Node
}

// SkipList is used in memtable.. It is a lock-free implementation of Skiplist.
// It is important to have a lock-free implementation,
// otherwise scan operation will take lock(s) (/read-locks) which will start interfering with write operations.
type SkipList struct {
	Head  *Node
	Level int
}

func NewSkipList() *SkipList {
	return &SkipList{
		Head: &Node{
			Pair:    kv.KeyValuePair{},
			Forward: make([]*Node, maxLevel),
		},
		Level: 0,
	}
}

func (s *SkipList) randomLevel() int {
	lv := 1
	for lv < maxLevel && rand.Float64() < pFactor {
		lv++
	}
	return lv
}

// Search 在跳表中搜索一个元素
// 判断key大小，来逐层查找
func (s *SkipList) Search(key kv.Key) (kv.Value, bool) {
	curr := s.Head
	for i := s.Level - 1; i >= 0; i-- {
		// 找到第 i 层小于且最接近 key 的元素
		for curr.Forward[i] != nil && curr.Forward[i].Pair.Key < key {
			curr = curr.Forward[i]
		}
	}
	curr = curr.Forward[0]
	// 检测当前元素的值是否等于 key
	if curr == nil || curr.Pair.Key != key {
		return nil, false
	}
	if curr.Pair.IsDeleted() {
		// 如果是逻辑删除的元素，返回 nil
		return nil, true
	}

	return curr.Pair.Value, true
}

// Add 向跳表中添加一个元素。
// 如果 key 已存在，则更新其值（并清除删除标记）；否则插入新的节点。
func (s *SkipList) Add(value kv.KeyValuePair) {
	update := make([]*Node, maxLevel)
	curr := s.Head
	// 同 Seek 一样，从最高层查找
	for i := s.Level - 1; i >= 0; i-- {
		for curr.Forward[i] != nil && curr.Forward[i].Pair.Key < value.Key {
			curr = curr.Forward[i]
		}
		update[i] = curr
	}
	// 检查是否已存在该 key
	next := curr.Forward[0]
	if next != nil && next.Pair.Key == value.Key {
		// 更新值，并取消删除标记
		next.Pair = value
		return
	}
	// 插入新的节点
	lv := s.randomLevel()
	if lv > s.Level {
		// 对于新增层级，将 Head 作为更新节点
		for i := s.Level; i < lv; i++ {
			update[i] = s.Head
		}
		s.Level = lv
	}
	newNode := &Node{
		Pair:    value,
		Forward: make([]*Node, lv),
	}
	// 更新各层 Forward 指针
	for i := 0; i < lv; i++ {
		newNode.Forward[i] = update[i].Forward[i]
		update[i].Forward[i] = newNode
	}
}

// Delete 删除跳表中指定 key 对应的节点。
// 这里采用逻辑删除（设置 Deleted 标记），同时更新 Forward 指针以便后续遍历跳过删除节点。
// 返回 true 表示成功删除，false 表示 key 不存在。
func (s *SkipList) Delete(key kv.Key) bool {
	update := make([]*Node, maxLevel)
	curr := s.Head
	// 查找待删除节点的前驱节点
	for i := s.Level - 1; i >= 0; i-- {
		for curr.Forward[i] != nil && curr.Forward[i].Pair.Key < key {
			curr = curr.Forward[i]
		}
		update[i] = curr
	}
	target := curr.Forward[0]
	if target == nil || target.Pair.Key != key {
		return false
	}
	target.Pair.Value = kv.DeletedValue
	// 更新各层 Forward 指针，直接跳过已删除节点
	for i := 0; i < s.Level; i++ {
		if update[i].Forward[i] != target {
			break
		}
		update[i].Forward[i] = target.Forward[i]
	}
	// 调整跳表的层级，确保最高层至少有一个节点
	for s.Level > 1 && s.Head.Forward[s.Level-1] == nil {
		s.Level--
	}
	return true
}

// First 返回跳表中第一个非删除的有效元素（最小 key）
// 如果跳表为空或只包含逻辑删除的节点，则返回 nil。
func (s *SkipList) First() *kv.KeyValuePair {
	curr := s.Head.Forward[0]
	for curr != nil {
		if !curr.Pair.IsDeleted() {
			return &curr.Pair
		}
		curr = curr.Forward[0]
	}
	return nil
}
