package block

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
)

// IndexEntry 表示一个 key 及对应value在数据块中的位置信息
type IndexEntry struct {
	Key    kv.Key
	Offset int64
}

// IndexBlock 表示 SSTable 的索引块，按 Key 顺序记录每个 DataBlock 的范围和位置信息
type IndexBlock struct {
	Indexes []*IndexEntry // 按 Key 升序排列的索引项
}

func NewIndexBlock() *IndexBlock {
	return &IndexBlock{
		Indexes: make([]*IndexEntry, 0),
	}
}

// Encode 将索引条目编码为小端字节流
func (e *IndexEntry) Encode(w io.Writer) (int64, error) {
	size, err := e.Key.EncodeTo(w)
	if err != nil {
		log.Errorf("encode index key failed: %s", err)
		return 0, err
	}

	// 编码Offset（8字节小端）
	if err := binary.Write(w, binary.LittleEndian, e.Offset); err != nil {
		log.Errorf("encode index offset failed: %s", err)
		return 0, fmt.Errorf("encode index offset failed: %w", err)
	}

	return size + 8, nil // 返回编码后的总字节数（key长度 + 8字节的offset）
}

// Encode 将整个索引块编码为字节流
func (b *IndexBlock) Encode(w io.Writer) (int64, error) {
	var totalSize int64 // 记录总字节数
	for _, entry := range b.Indexes {
		size, err := entry.Encode(w)
		if err != nil {
			log.Errorf("encode index entry failed: %s", err.Error())
			return 0, fmt.Errorf("encode index entry failed: %w", err)
		}
		totalSize += size
	}
	return totalSize, nil
}

// DecodeFrom 从字节流解码索引块
func (b *IndexBlock) DecodeFrom(r io.Reader, size int64) error {
	var totalRead int64 // 记录已读取的总字节数
	b.Indexes = make([]*IndexEntry, 0)

	for {
		// 检查是否已读取足够的数据
		if size > 0 && totalRead >= size {
			break
		}

		// 1. 读取Key
		var key kv.Key
		keySize, err := key.DecodeFrom(r)
		if err != nil {
			log.Errorf("decode index key length failed: %s", err.Error())
			return fmt.Errorf("decode index key length failed: %w", err)
		}
		totalRead += keySize
		// 检查是否超出大小限制
		if size > 0 && totalRead >= size {
			log.Errorf("unexpected EOF: size limit reached while reading key length")
			return fmt.Errorf("unexpected EOF: size limit reached while reading key length")
		}

		// 2. 读取Offset（8字节小端）
		var offset int64
		if err := binary.Read(r, binary.LittleEndian, &offset); err != nil {
			log.Errorf("decode index offset failed: %s", err.Error())
			return fmt.Errorf("decode index offset failed: %w", err)
		}
		totalRead += 8

		// 构造索引条目
		b.Indexes = append(b.Indexes, &IndexEntry{
			Key:    key,
			Offset: offset,
		})
	}

	// 如果指定了size但读取的字节数不足，可能是数据不完整
	if totalRead < size {
		log.Errorf("unexpected EOF: incomplete index block, read %d of %d bytes", totalRead, size)
		return fmt.Errorf("unexpected EOF: incomplete index block, read %d of %d bytes", totalRead, size)
	}

	return nil
}

func (b *IndexBlock) Add(key kv.Key, offset int64) {
	b.Indexes = append(b.Indexes, &IndexEntry{
		Key:    key,
		Offset: offset,
	})
}

func (b *IndexBlock) Len() int {
	return len(b.Indexes)
}

// Iterator 实现了 database.Iterator 接口，用于遍历 IndexBlock
type Iterator struct {
	indexBlock *IndexBlock
	current    int // 当前索引位置
}

// NewIterator 创建一个新的 IndexBlock 迭代器
func NewIterator(indexBlock *IndexBlock) *Iterator {
	return &Iterator{
		indexBlock: indexBlock,
		current:    -1, // 初始化为-1，这样第一次Next()会移动到0
	}
}

// Valid 检查迭代器当前位置是否有效
func (i *Iterator) Valid() bool {
	return i.current >= 0 && i.current < len(i.indexBlock.Indexes)
}

// Key 返回当前索引条目的key
func (i *Iterator) Key() kv.Key {
	if !i.Valid() {
		return ""
	}
	return i.indexBlock.Indexes[i.current].Key
}

// Value 返回nil，因为IndexBlock不存储值
func (i *Iterator) Value() kv.Value {
	return nil
}

// Next 将迭代器移动到下一个索引条目
func (i *Iterator) Next() {
	if i.current+1 >= len(i.indexBlock.Indexes) {
		i.current = len(i.indexBlock.Indexes) // 设置为无效位置
		return
	}
	i.current++
}

// Seek 查找与目标key完全匹配的索引条目
// 如果找到完全匹配的key，则定位到该条目；否则设置为无效状态
func (i *Iterator) Seek(target kv.Key) {
	// 使用二分查找来快速定位
	left, right := 0, len(i.indexBlock.Indexes)
	for left < right {
		mid := left + (right-left)/2
		if i.indexBlock.Indexes[mid].Key < target {
			left = mid + 1
		} else {
			right = mid
		}
	}

	// 检查是否越界
	if left >= len(i.indexBlock.Indexes) {
		i.current = len(i.indexBlock.Indexes) // 设置为无效位置
		return
	}

	// 检查是否完全匹配（因为二分查找可能停在大于或等于的位置）
	if i.indexBlock.Indexes[left].Key == target {
		i.current = left // 完全匹配，设置当前位置
	} else {
		i.current = len(i.indexBlock.Indexes) // 未找到完全匹配，设置为无效位置
	}
}

// SeekToFirst 将迭代器移动到第一个索引条目
func (i *Iterator) SeekToFirst() {
	if len(i.indexBlock.Indexes) == 0 {
		i.current = -1
		return
	}
	i.current = 0
}

// SeekToLast 将迭代器移动到最后一个索引条目
func (i *Iterator) SeekToLast() {
	if len(i.indexBlock.Indexes) == 0 {
		i.current = -1
		return
	}
	i.current = len(i.indexBlock.Indexes) - 1
}

func (i *Iterator) KeyIndex() int {
	// 返回当前索引位置
	return i.current
}

func (i *Iterator) ValueOffset() int64 {
	return i.indexBlock.Indexes[i.current].Offset
}

// Close 实现空操作，因为IndexBlockIterator不需要清理资源
func (i *Iterator) Close() {
}
