// 定义 kv 和 存储方式
// 采用小端存储，使用长度前缀编码
/*
┌────────────┬──────────┬──────────────┬────────────┐
│ key length │ key data │ value length │ value data │
└────────────┴──────────┴──────────────┴────────────┘
*/

package kv

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/xmh1011/go-lsm/log"
)

type Key string
type Value []byte

type KeyValuePair struct {
	Key   Key
	Value Value
}

const deletedValueStr = "～DELETED～"

var DeletedValue = Value(deletedValueStr)

func (p *KeyValuePair) Copy() *KeyValuePair {
	return &KeyValuePair{
		Key:   p.Key,
		Value: p.Value,
	}
}

func (p *KeyValuePair) IsDeleted() bool {
	// 判断 Value 是否为删除标记
	return p.Value != nil && string(p.Value) == deletedValueStr
}

// EncodeTo 使用4字节小端编码
func (p *KeyValuePair) EncodeTo(w io.Writer) error {
	// 编码 key 长度（4字节小端）
	keyLen := uint32(len(p.Key))
	if err := binary.Write(w, binary.LittleEndian, keyLen); err != nil {
		log.Errorf("write key length failed: %s", err)
		return fmt.Errorf("encode key length: %w", err)
	}

	// 编码 key 数据
	if _, err := w.Write([]byte(p.Key)); err != nil {
		log.Errorf("write key bytes failed: %s", err)
		return fmt.Errorf("encode key: %w", err)
	}

	// 编码 value 长度（4字节小端）
	valLen := uint32(len(p.Value))
	if err := binary.Write(w, binary.LittleEndian, valLen); err != nil {
		log.Errorf("write value length failed: %s", err)
		return fmt.Errorf("encode value length: %w", err)
	}

	// 编码 value 数据
	if _, err := w.Write(p.Value); err != nil {
		log.Errorf("write value bytes failed: %s", err)
		return fmt.Errorf("encode value: %w", err)
	}

	return nil
}

// DecodeFrom 使用4字节小端解码
func (p *KeyValuePair) DecodeFrom(r io.Reader) error {
	// 解码 key 长度（4字节小端）
	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		log.Errorf("read key length failed: %s", err)
		return fmt.Errorf("decode key length: %w", err)
	}
	if keyLen > 1<<20 {
		return fmt.Errorf("invalid key length: %d", keyLen)
	}

	// 解码 key 数据
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		log.Errorf("read key failed: %s", err)
		return fmt.Errorf("decode key: %w", err)
	}
	p.Key = Key(key)

	// 解码 value 长度（4字节小端）
	var valLen uint32
	if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
		log.Errorf("read value length failed: %s", err)
		return fmt.Errorf("decode value length: %w", err)
	}
	if valLen > 1<<30 {
		return fmt.Errorf("invalid value length: %d", valLen)
	}

	// 解码 value 数据
	val := make([]byte, valLen)
	if _, err := io.ReadFull(r, val); err != nil {
		log.Errorf("read value failed: %s", err)
		return fmt.Errorf("decode value: %w", err)
	}
	p.Value = val

	return nil
}

// EstimateSize 估算编码后大小
func (p *KeyValuePair) EstimateSize() uint64 {
	// 4字节 key 长度 + key 数据长度 + 4字节 value 长度 + value 数据长度 + 8字节 value offset
	return 4 + uint64(len(p.Key)) + 4 + uint64(len(p.Value)) + 8
}

// DecodeFrom 从 io.Reader 解码 Key（小端存储 + 4字节长度前缀）
func (k *Key) DecodeFrom(r io.Reader) (int64, error) {
	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		log.Errorf("read key keyLen failed: %s", err)
		return 0, fmt.Errorf("decode key keyLen: %w", err)
	}

	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(r, keyBytes); err != nil {
		log.Errorf("read key bytes failed: %s", err)
		return 0, fmt.Errorf("decode key bytes: %w", err)
	}

	*k = Key(keyBytes)
	return int64(4 + len(keyBytes)), nil
}

// EncodeTo 编码 Key（小端存储 + 4字节长度前缀），并返回写入的字节数
func (k *Key) EncodeTo(w io.Writer) (int64, error) {
	var totalWritten int64

	// 1. 写入 Key 长度（4字节小端）
	keyLen := uint32(len(*k))
	if err := binary.Write(w, binary.LittleEndian, keyLen); err != nil {
		log.Errorf("write key length failed: %s", err.Error())
		return totalWritten, fmt.Errorf("encode key length: %w", err)
	}
	totalWritten += 4

	// 2. 写入 Key 数据
	n, err := w.Write([]byte(*k))
	if err != nil {
		log.Errorf("write key bytes failed: %s", err.Error())
		return totalWritten, fmt.Errorf("encode key bytes: %w", err)
	}
	totalWritten += int64(n)

	return totalWritten, nil
}

// EncodeTo 编码 Value（小端存储 + 4字节长度前缀）
func (v *Value) EncodeTo(w io.Writer) (int64, error) {
	valLen := uint32(len(*v))
	if err := binary.Write(w, binary.LittleEndian, valLen); err != nil {
		log.Errorf("write value length failed: %s", err)
		return 0, fmt.Errorf("encode value length: %w", err)
	}

	if _, err := w.Write(*v); err != nil {
		log.Errorf("write value bytes failed: %s", err)
		return 0, fmt.Errorf("encode value: %w", err)
	}

	return int64(4 + valLen), nil // 返回编码后的总字节数（4字节长度 + value数据长度）
}

// DecodeFrom 从 io.Reader 解码 Value（小端存储 + 4字节长度前缀）
func (v *Value) DecodeFrom(r io.Reader) error {
	var valLen uint32
	if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
		log.Errorf("read value length failed: %s", err)
		return fmt.Errorf("decode value length: %w", err)
	}

	if valLen > 1<<30 { // 1GB
		return fmt.Errorf("invalid value length: %d", valLen)
	}

	val := make([]byte, valLen)
	if _, err := io.ReadFull(r, val); err != nil {
		log.Errorf("read value bytes failed: %s", err)
		return fmt.Errorf("decode value: %w", err)
	}

	*v = val
	return nil
}
