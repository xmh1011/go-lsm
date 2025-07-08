package sstable

import (
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/sstable/block"
)

type Iterator struct {
	SSTable       *SSTable
	IndexIterator *block.Iterator // 索引迭代器，用于查找数据块
}

// NewSSTableIterator 创建一个新的 SSTable 迭代器
func NewSSTableIterator(sst *SSTable) *Iterator {
	it := &Iterator{
		SSTable:       sst,
		IndexIterator: block.NewIterator(sst.IndexBlock), // 使用 SSTable 的索引块创建迭代器
	}
	it.SeekToFirst()
	return it
}

// Valid 检查迭代器当前位置是否有效
func (i *Iterator) Valid() bool {
	return i.IndexIterator.Valid()
}

// Key 返回当前索引条目的key
func (i *Iterator) Key() kv.Key {
	return i.IndexIterator.Key()
}

// Value 返回
func (i *Iterator) Value() (kv.Value, error) {
	if !i.Valid() {
		return nil, nil // 如果迭代器无效，返回nil
	}

	value, err := i.SSTable.GetValueByOffset(i.IndexIterator.ValueOffset())
	if err != nil {
		log.Errorf("failed to get value by offset error: %s", err.Error())
		return nil, err // 如果获取值失败，返回错误
	}

	return value, nil
}

// Next 将迭代器移动到下一个索引条目
func (i *Iterator) Next() {
	i.IndexIterator.Next()
}

// Seek 查找大于或等于目标key的第一个索引条目（使用二分查找）
func (i *Iterator) Seek(target kv.Key) {
	i.IndexIterator.Seek(target)
}

// SeekToFirst 将迭代器移动到第一个索引条目
func (i *Iterator) SeekToFirst() {
	i.IndexIterator.SeekToFirst()
}

// SeekToLast 将迭代器移动到最后一个索引条目
func (i *Iterator) SeekToLast() {
	i.IndexIterator.SeekToLast()
}

// Close 关闭迭代器，释放相关资源
func (i *Iterator) Close() {
	i.SSTable = nil         // 清理 SSTable 引用
	i.IndexIterator.Close() // 关闭索引迭代器
}
