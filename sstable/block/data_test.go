package block

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
)

func TestDataBlock_EncodeDecode(t *testing.T) {
	tests := []struct {
		name      string
		entries   []kv.Value
		sizeLimit int64 // 0 表示不限制
		wantErr   bool
	}{
		{
			name:      "empty block",
			entries:   []kv.Value{},
			sizeLimit: 0,
			wantErr:   false,
		},
		{
			name:      "single entry",
			entries:   []kv.Value{[]byte("value1")},
			sizeLimit: 0,
			wantErr:   false,
		},
		{
			name:      "multiple entries",
			entries:   []kv.Value{[]byte("value1"), []byte("value2"), []byte("value3")},
			sizeLimit: 0,
			wantErr:   false,
		},
		{
			name:      "with size limit (sufficient)",
			entries:   []kv.Value{[]byte("value1"), []byte("value2")},
			sizeLimit: 100, // 足够大的限制
			wantErr:   false,
		},
		{
			name:    "with size limit (exact)",
			entries: []kv.Value{[]byte("value1"), []byte("value2")},
			sizeLimit: func() int64 { // 计算精确大小：2*(4字节长度 + 6字节数据)
				total := int64(0)
				for _, e := range []kv.Value{[]byte("value1"), []byte("value2")} {
					total += 4 + int64(len(e))
				}
				return total
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 准备测试数据
			block := NewDataBlock()
			for _, e := range tt.entries {
				block.Add(e)
			}

			// 编码
			buf := &bytes.Buffer{}
			err := block.EncodeTo(buf)
			assert.Equal(t, tt.wantErr, err != nil, "EncodeTo error mismatch")
			if tt.wantErr {
				return
			}

			// 解码（带可选大小限制）
			decodedBlock := NewDataBlock()
			reader := bytes.NewReader(buf.Bytes())
			err = decodedBlock.DecodeFrom(reader, tt.sizeLimit)
			assert.NoError(t, err, "DecodeFrom should not error")

			// 验证解码结果
			assert.Equal(t, len(tt.entries), decodedBlock.Len(), "Entry count mismatch")
			for i, expected := range tt.entries {
				assert.Equal(t, expected, decodedBlock.Entries[i], "Entry %d mismatch", i)
			}
		})
	}
}

func TestDataBlock_DecodeWithSizeLimit(t *testing.T) {
	// 创建一个包含多个条目的块
	block := NewDataBlock()
	block.Add([]byte("value1"))
	block.Add([]byte("value2"))
	block.Add([]byte("value3"))

	// 编码到缓冲区
	buf := &bytes.Buffer{}
	err := block.EncodeTo(buf)
	assert.NoError(t, err, "EncodeTo should not error")

	// 测试不足的大小限制
	t.Run("insufficient size limit", func(t *testing.T) {
		// 计算只够读取第一个条目的大小限制
		firstEntrySize := 4 + len(block.Entries[0]) // 4字节长度 + 数据
		insufficientLimit := int64(firstEntrySize - 1)

		decodedBlock := NewDataBlock()
		reader := bytes.NewReader(buf.Bytes())
		err := decodedBlock.DecodeFrom(reader, insufficientLimit)

		assert.Error(t, err, "DecodeFrom should error with insufficient size limit")
	})

	// 测试精确的大小限制（刚好读取两个条目）
	t.Run("partial read with size limit", func(t *testing.T) {
		// 计算两个条目的大小
		partialSize := int64(0)
		for i := 0; i < 2; i++ {
			partialSize += 4 + int64(len(block.Entries[i]))
		}

		decodedBlock := NewDataBlock()
		reader := bytes.NewReader(buf.Bytes())
		err := decodedBlock.DecodeFrom(reader, partialSize)

		assert.NoError(t, err, "DecodeFrom should not error with exact size limit")
		assert.Equal(t, 2, decodedBlock.Len(), "Should read exactly 2 entries")
		assert.Equal(t, block.Entries[0], decodedBlock.Entries[0], "First entry mismatch")
		assert.Equal(t, block.Entries[1], decodedBlock.Entries[1], "Second entry mismatch")
	})
}

func TestDataBlock_DecodeCorruptedData(t *testing.T) {
	// 测试损坏的数据（长度字段不正确）
	t.Run("invalid length prefix", func(t *testing.T) {
		// 创建包含无效长度前缀的数据
		buf := &bytes.Buffer{}
		// 写入一个过大的长度前缀（4字节）
		binary.Write(buf, binary.LittleEndian, uint32(999999))

		decodedBlock := NewDataBlock()
		err := decodedBlock.DecodeFrom(buf, 0)

		assert.Error(t, err, "DecodeFrom should error with invalid length prefix")
	})

	// 测试不完整的数据（长度正确但数据不完整）
	t.Run("incomplete data", func(t *testing.T) {
		buf := &bytes.Buffer{}
		// 写入长度前缀（4字节）
		binary.Write(buf, binary.LittleEndian, uint32(10)) // 需要10字节数据
		// 但只写入5字节数据
		buf.Write([]byte("incom"))

		decodedBlock := NewDataBlock()
		err := decodedBlock.DecodeFrom(buf, 0)

		assert.Error(t, err, "DecodeFrom should error with incomplete data")
	})
}

func TestDataBlock_AddAndLen(t *testing.T) {
	block := NewDataBlock()
	assert.Equal(t, 0, block.Len(), "New block should be empty")

	// 添加条目
	block.Add([]byte("value1"))
	assert.Equal(t, 1, block.Len(), "Len should be 1 after adding one entry")

	block.Add([]byte("value2"))
	assert.Equal(t, 2, block.Len(), "Len should be 2 after adding two entries")

	// 验证条目内容
	assert.Equal(t, kv.Value("value1"), block.Entries[0], "First entry mismatch")
	assert.Equal(t, kv.Value("value2"), block.Entries[1], "Second entry mismatch")
}
