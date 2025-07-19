package block

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
)

func TestIndexEntry_Encode(t *testing.T) {
	tests := []struct {
		name     string
		key      kv.Key
		offset   int64
		expected []byte
	}{
		{
			name:   "normal_case",
			key:    "key1",
			offset: 123,
			expected: func() []byte {
				buf := new(bytes.Buffer)
				err := binary.Write(buf, binary.LittleEndian, uint32(4)) // key length
				assert.NoError(t, err)
				buf.Write([]byte("key1"))                                // key
				err = binary.Write(buf, binary.LittleEndian, int64(123)) // offset
				assert.NoError(t, err)
				return buf.Bytes()
			}(),
		},
		{
			name:   "empty_key",
			key:    "",
			offset: 0,
			expected: func() []byte {
				buf := new(bytes.Buffer)
				err := binary.Write(buf, binary.LittleEndian, uint32(0)) // key length
				assert.NoError(t, err)
				err = binary.Write(buf, binary.LittleEndian, int64(0)) // offset
				assert.NoError(t, err)
				return buf.Bytes()
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &IndexEntry{Key: tt.key, Offset: tt.offset}
			buf := new(bytes.Buffer)
			_, err := entry.Encode(buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, buf.Bytes())
		})
	}
}

func TestIndexBlock_EncodeDecode(t *testing.T) {
	// 创建测试数据
	block := NewIndexBlock()
	block.Add("key1", 100)
	block.Add("key2", 200)
	block.Add("key3", 300)

	// 编码
	buf := new(bytes.Buffer)
	size, err := block.Encode(buf)
	assert.NoError(t, err)

	// 解码（关键修复点：使用 bytes.Reader 并确保数据正确）
	data := buf.Bytes()
	decodedBlock := NewIndexBlock()
	// 验证非法size
	err = decodedBlock.DecodeFrom(bytes.NewReader(data), -1)
	assert.Error(t, err, "DecodeFrom should return error for negative size")
	err = decodedBlock.DecodeFrom(bytes.NewReader(data), size)
	assert.NoError(t, err)

	// 验证解码后的数据
	assert.Equal(t, 3, len(decodedBlock.Indexes))
	assert.Equal(t, kv.Key("key1"), decodedBlock.Indexes[0].Key)
	assert.Equal(t, int64(100), decodedBlock.Indexes[0].Offset)
	assert.Equal(t, kv.Key("key2"), decodedBlock.Indexes[1].Key)
	assert.Equal(t, int64(200), decodedBlock.Indexes[1].Offset)
	assert.Equal(t, kv.Key("key3"), decodedBlock.Indexes[2].Key)
	assert.Equal(t, int64(300), decodedBlock.Indexes[2].Offset)
}

func TestIndexBlock_Iterator(t *testing.T) {
	block := NewIndexBlock()
	block.Add("apple", 10)
	block.Add("banana", 20)
	block.Add("cherry", 30)

	iter := NewIterator(block)

	// 测试 SeekToFirst
	iter.SeekToFirst()
	assert.True(t, iter.Valid())
	assert.Equal(t, kv.Key("apple"), iter.Key())
	assert.Equal(t, int64(10), iter.ValueOffset())

	// 测试 Next
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, kv.Key("banana"), iter.Key())
	assert.Equal(t, int64(20), iter.ValueOffset())

	// 测试 SeekToLast
	iter.SeekToLast()
	assert.True(t, iter.Valid())
	assert.Equal(t, kv.Key("cherry"), iter.Key())
	assert.Equal(t, int64(30), iter.ValueOffset())

	// 测试 Seek（精确匹配）
	iter.Seek("banana")
	assert.True(t, iter.Valid())
	assert.Equal(t, kv.Key("banana"), iter.Key())
	assert.Equal(t, int64(20), iter.ValueOffset())

	// 测试 Seek（无匹配）
	iter.Seek("orange")
	assert.False(t, iter.Valid())
}

func TestIndexBlock_DecodeWithSizeLimit(t *testing.T) {
	// 创建包含 2 个 entry 的 block
	block := NewIndexBlock()
	block.Add("key1", 100)
	block.Add("key2", 200)

	// 编码
	buf := new(bytes.Buffer)
	_, err := block.Encode(buf)
	assert.NoError(t, err)
	fullData := buf.Bytes()

	// 测试部分解码（仅第一个 entry）
	firstEntrySize := int64(4 + len("key1") + 8) // keyLen + key + offset
	limitedBlock := NewIndexBlock()
	err = limitedBlock.DecodeFrom(bytes.NewReader(fullData), firstEntrySize)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(limitedBlock.Indexes))
	assert.Equal(t, kv.Key("key1"), limitedBlock.Indexes[0].Key)
	assert.Equal(t, int64(100), limitedBlock.Indexes[0].Offset)

	// 测试数据不完整的情况
	incompleteData := fullData[:firstEntrySize-2] // 故意截断
	incompleteBlock := NewIndexBlock()
	err = incompleteBlock.DecodeFrom(bytes.NewReader(incompleteData), firstEntrySize)
	assert.Error(t, err)
}
