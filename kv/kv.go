// 定义 kv 和 存储方式
// 采用小端存储，使用长度前缀编码
/*
┌────────────┬──────────┬──────────────┬────────────┐
│ key length │ key data │ value length │ value data │
└────────────┴──────────┴──────────────┴────────────┘
*/

package kv

import (
	"fmt"
	"io"

	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/util"
)

type Key string
type Value []byte

type KeyValuePair struct {
	Key     Key
	Value   Value
	Deleted bool
}

func (p *KeyValuePair) Copy() *KeyValuePair {
	return &KeyValuePair{
		Key:     p.Key,
		Value:   p.Value,
		Deleted: p.Deleted,
	}
}

// EncodeTo 用 uvarint 编码 key/value 长度
func (p *KeyValuePair) EncodeTo(w io.Writer) error {
	// 编码 key 长度
	keyBytes := []byte(p.Key)
	if err := util.WriteUvarint(w, uint64(len(keyBytes))); err != nil {
		log.Errorf("write key length failed: %s", err)
		return fmt.Errorf("encode key length: %w", err)
	}
	if _, err := w.Write(keyBytes); err != nil {
		log.Errorf("write key bytes failed: %s", err)
		return fmt.Errorf("encode key: %w", err)
	}

	// 编码 value 长度
	if err := util.WriteUvarint(w, uint64(len(p.Value))); err != nil {
		log.Errorf("write value length failed: %s", err)
		return fmt.Errorf("encode value length: %w", err)
	}
	if _, err := w.Write(p.Value); err != nil {
		log.Errorf("write value bytes failed: %s", err)
		return fmt.Errorf("encode value: %w", err)
	}

	// 写 deleted 字节
	var deletedByte byte
	if p.Deleted {
		deletedByte = 1
	}
	if _, err := w.Write([]byte{deletedByte}); err != nil {
		log.Errorf("write deleted byte failed: %s", err)
		return fmt.Errorf("encode deleted: %w", err)
	}

	return nil
}

// DecodeFrom 从 reader 解码 KeyValuePair（uvarint + data）
func (p *KeyValuePair) DecodeFrom(r io.Reader) error {
	// 解码 key 长度
	keyLen, err := util.ReadUvarint(r)
	if err != nil {
		log.Errorf("read key length failed: %s", err)
		return fmt.Errorf("decode key length: %w", err)
	}
	if keyLen > 1<<20 {
		return fmt.Errorf("invalid key length: %d", keyLen)
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		log.Errorf("read key failed: %s", err)
		return fmt.Errorf("decode key: %w", err)
	}
	p.Key = Key(key)

	// 解码 value 长度
	valLen, err := util.ReadUvarint(r)
	if err != nil {
		log.Errorf("read value length failed: %s", err)
		return fmt.Errorf("decode value length: %w", err)
	}
	if valLen > 1<<30 {
		return fmt.Errorf("invalid value length: %d", valLen)
	}
	val := make([]byte, valLen)
	if _, err := io.ReadFull(r, val); err != nil {
		log.Errorf("read value failed: %s", err)
		return fmt.Errorf("decode value: %w", err)
	}
	p.Value = val

	// 读取 deleted 字节
	var deleted [1]byte
	if _, err := io.ReadFull(r, deleted[:]); err != nil {
		log.Errorf("read deleted byte failed: %s", err)
		return fmt.Errorf("decode deleted: %w", err)
	}
	p.Deleted = deleted[0] == 1

	return nil
}

// EstimateSize 估算 KeyValuePair 的大小
// UVarint 最多 5 bytes
// deleted 1 byte
func (p *KeyValuePair) EstimateSize() uint64 {
	return uint64(1 + 5 + len(p.Key) + 5 + len(p.Value))
}

// DecodeFrom 解码 Key（使用 uvarint 编码长度）
func (k *Key) DecodeFrom(r io.Reader) error {
	keyLen, err := util.ReadUvarint(r)
	if err != nil {
		log.Errorf("read key length failed: %s", err)
		return fmt.Errorf("decode key length: %w", err)
	}
	if keyLen > 1<<20 {
		return fmt.Errorf("invalid key length: %d", keyLen)
	}
	data := make([]byte, keyLen)
	if _, err := io.ReadFull(r, data); err != nil {
		log.Errorf("read key bytes failed: %s", err)
		return fmt.Errorf("decode key: %w", err)
	}
	*k = Key(data)
	return nil
}

// EncodeTo 编码 Key 长度 + 内容（使用 uvarint 编码）
func (k *Key) EncodeTo(w io.Writer) error {
	keyBytes := []byte(*k)
	if err := util.WriteUvarint(w, uint64(len(keyBytes))); err != nil {
		log.Errorf("write key length failed: %s", err)
		return fmt.Errorf("encode key length: %w", err)
	}
	if _, err := w.Write(keyBytes); err != nil {
		log.Errorf("write key bytes failed: %s", err)
		return fmt.Errorf("encode key bytes: %w", err)
	}
	return nil
}
