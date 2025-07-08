// MemTable Iterator 的实现
// 因为 SSTable 是有序文件结构，必须从 MemTable 中拿到升序遍历的迭代器
// 顺序写入所有 key/value
// Compaction 合并多个 SSTable 或 MemTable
// 多路归并排序场景需要对多个 MemTable/SSTable 并发迭代
// TODO: 支持 Range Query（范围查询）

package memtable

import (
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/memtable/skiplist"
)

// Iterator 封装了跳表迭代器，提供统一的遍历接口。
type Iterator struct {
	iter *skiplist.Iterator
}

// NewMemTableIterator 返回一个 Iterator，用于有序遍历 MemTable 中的数据。
// 迭代器基于底层 SkipList 的迭代器实现。
func NewMemTableIterator(l *skiplist.SkipList) *Iterator {
	iter := skiplist.NewSkipListIterator(l)
	iter.SeekToFirst() // 定位到第一个有效节点
	return &Iterator{iter: iter}
}

// Valid 返回迭代器是否处于有效节点。
func (i *Iterator) Valid() bool {
	return i.iter.Valid()
}

// Seek 将迭代器定位到指定的 key。
func (i *Iterator) Seek(key kv.Key) {
	i.iter.Seek(key)
}

// SeekToFirst 将迭代器定位到第一个节点。
func (i *Iterator) SeekToFirst() {
	i.iter.SeekToFirst()
}

// SeekToLast 将迭代器定位到最后一个节点。
func (i *Iterator) SeekToLast() {
	i.iter.SeekToLast()
}

// Next 将迭代器移动到下一个节点。
func (i *Iterator) Next() {
	i.iter.Next()
}

// Key 返回当前节点的 key。
func (i *Iterator) Key() kv.Key {
	return i.iter.Key()
}

// Value 返回当前节点的 value。
func (i *Iterator) Value() kv.Value {
	return i.iter.Value()
}

// Close 关闭迭代器，释放相关资源
func (i *Iterator) Close() {
	i.iter.Close()
}
