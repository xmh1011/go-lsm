package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
)

// DataBlock 表示一个数据块，包含多个键值对
type DataBlock struct {
	Entries []kv.Value
}

// NewDataBlock 创建新的 DataBlock
func NewDataBlock() *DataBlock {
	return &DataBlock{
		Entries: make([]kv.Value, 0),
	}
}

// EncodeTo 将 DataBlock 编码到 writer
func (b *DataBlock) EncodeTo(w io.Writer) error {
	buf := &bytes.Buffer{}

	// 编码所有键值对
	for _, entry := range b.Entries {
		if _, err := entry.EncodeTo(buf); err != nil {
			log.Errorf("encode entry failed: %s", err.Error())
			return fmt.Errorf("encode entry: %w", err)
		}
	}

	// 一次性写入所有数据
	_, err := w.Write(buf.Bytes())
	if err != nil {
		log.Errorf("write data block failed: %s", err.Error())
		return fmt.Errorf("write data block: %w", err)
	}

	return nil
}

// DecodeFrom 从 reader 解码 DataBlock，支持偏移量和大小限制
// - size: 最大读取字节数（0 表示不限制）
func (b *DataBlock) DecodeFrom(r io.Reader, size int64) error {
	// 限制读取大小的 Reader 包装器
	var limitedReader = r
	if size > 0 {
		limitedReader = io.LimitReader(r, size)
	}

	// 解码逻辑：持续读取键值对，直到数据结束或达到大小限制
	bufReader := limitedReader
	for {
		// 1. 读取 Value 长度（4字节）
		var valLen uint32
		if err := binary.Read(bufReader, binary.LittleEndian, &valLen); err != nil {
			if err == io.EOF {
				break // 正常结束
			}
			log.Errorf("read value length failed: %s", err.Error())
			return fmt.Errorf("read value length failed: %w", err)
		}

		// 2. 读取 Value 数据
		value := make(kv.Value, valLen)
		if _, err := io.ReadFull(bufReader, value); err != nil {
			log.Errorf("read value failed: %s", err.Error())
			return fmt.Errorf("read value data failed: %w", err)
		}
		b.Add(value)
	}

	return nil
}

func (b *DataBlock) Add(value kv.Value) {
	b.Entries = append(b.Entries, value)
}

func (b *DataBlock) Len() int {
	return len(b.Entries)
}
