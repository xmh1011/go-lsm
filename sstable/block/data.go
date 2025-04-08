/*
Data Records structure:
┌────────────┬──────────┬──────────────┬────────────┐
│ key length │ key data │ value length │ value data │
└────────────┴──────────┴──────────────┴────────────┘
*/

package block

import (
	"fmt"
	"io"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
)

const (
	maxDataBlockSize = 32 * 1024 // 32kb
)

// DataBlock 表示 SSTable 中的一个数据块，由多个 DataRecord 组成。
type DataBlock struct {
	Records []*kv.KeyValuePair
	size    uint64
}

func NewDataBlock() *DataBlock {
	return &DataBlock{
		Records: make([]*kv.KeyValuePair, 0),
	}
}

// DecodeFrom 从文件中读取数据块，并解析出其中的记录。
func (d *DataBlock) DecodeFrom(r io.ReadSeeker, handle Handle) error {
	if _, err := r.Seek(int64(handle.Offset), io.SeekStart); err != nil {
		log.Errorf("seek to data block failed: %s", err.Error())
		return fmt.Errorf("seek to data block failed: %w", err)
	}

	var bytesRead uint64
	d.Records = make([]*kv.KeyValuePair, 0)
	for bytesRead < handle.Size {
		startPos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Errorf("get current offset failed: %s", err.Error())
			return fmt.Errorf("get current offset failed: %w", err)
		}

		var pair kv.KeyValuePair
		if err := pair.DecodeFrom(r); err != nil {
			log.Errorf("decode handle failed: %s", err.Error())
			return fmt.Errorf("decode handle failed: %w", err)
		}
		d.Records = append(d.Records, &pair)

		endPos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Errorf("get end offset failed: %s", err.Error())
			return fmt.Errorf("get end offset failed: %w", err)
		}
		bytesRead += uint64(endPos - startPos)
	}

	if bytesRead != handle.Size {
		log.Errorf("index block bytes read (%d) != expected (%d)", bytesRead, handle.Size)
		return fmt.Errorf("index block bytes read (%d) != expected (%d)", bytesRead, handle.Size)
	}

	return nil
}

// EncodeTo 将 DataBlock 中的所有记录编码后写入 io.Writer，并返回写入的字节数。
func (d *DataBlock) EncodeTo(w io.WriteSeeker) (Handle, error) {
	var handle Handle
	// 记录当前起始写入偏移量
	startOffset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("get current offset failed: %s", err.Error())
		return handle, fmt.Errorf("get current offset failed: %w", err)
	}
	handle.Offset = uint64(startOffset)

	for _, pair := range d.Records {
		if err := pair.EncodeTo(w); err != nil {
			log.Errorf("encode key-value pair failed: %s", err.Error())
			return handle, fmt.Errorf("encode key-value pair failed: %w", err)
		}
	}

	// 记录当前写入结束偏移量
	endOffset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("get end offset failed: %s", err.Error())
		return handle, fmt.Errorf("get end offset failed: %w", err)
	}
	handle.Size = uint64(endOffset - startOffset)

	return handle, nil
}

func (d *DataBlock) EstimateSize() uint64 {
	return d.size
}

func (d *DataBlock) AddRecord(pair *kv.KeyValuePair) {
	d.Records = append(d.Records, pair)
	d.size += pair.EstimateSize()
}

func (d *DataBlock) CanInsert(pair *kv.KeyValuePair) bool {
	return d.EstimateSize()+pair.EstimateSize() < maxDataBlockSize
}

// DataIterator 用于遍历 DataBlock 中的记录
type DataIterator struct {
	block *DataBlock
	index int
}

// NewDataBlockIterator 创建并返回一个新的 DataBlock 迭代器，初始位于第一个记录之前（调用 SeekToFirst 激活）
func NewDataBlockIterator(block *DataBlock) *DataIterator {
	return &DataIterator{
		block: block,
		index: -1, // 初始状态：未定位
	}
}

// Valid 判断当前迭代器位置是否有效
func (i *DataIterator) Valid() bool {
	return i.block != nil && i.index >= 0 && i.index < len(i.block.Records)
}

// Key 返回当前记录的 Key
func (i *DataIterator) Key() kv.Key {
	if !i.Valid() {
		return ""
	}
	return i.block.Records[i.index].Key
}

// Value 返回当前记录的 Value
func (i *DataIterator) Value() kv.Value {
	if !i.Valid() {
		return nil
	}
	return i.block.Records[i.index].Value
}

// Pair 返回当前记录的 kv.KeyValuePair
func (i *DataIterator) Pair() *kv.KeyValuePair {
	if !i.Valid() {
		return nil
	}
	return i.block.Records[i.index]
}

// Next 移动到下一个记录
func (i *DataIterator) Next() {
	i.index++
}

// SeekToFirst 将迭代器移动到第一个记录
func (i *DataIterator) SeekToFirst() {
	i.index = 0
}

// SeekToLast 将迭代器移动到最后一个记录
func (i *DataIterator) SeekToLast() {
	i.index = len(i.block.Records) - 1
}

// Seek 定位到第一个 key ≥ 目标 key 的记录
func (i *DataIterator) Seek(target kv.Key) {
	left, right := 0, len(i.block.Records)
	for left < right {
		mid := (left + right) / 2
		if i.block.Records[mid].Key < target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	i.index = left
}

// Close 清理资源（内存迭代器无实际资源，但设为 nil 以防误用）
func (i *DataIterator) Close() {
	i.block = nil
}
