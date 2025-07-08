package block

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
)

func TestHeader_EncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		minKey  kv.Key
		maxKey  kv.Key
		wantErr bool
	}{
		{
			name:    "normal case",
			minKey:  kv.Key("key1"),
			maxKey:  kv.Key("key2"),
			wantErr: false,
		},
		{
			name:    "empty min key",
			minKey:  kv.Key(""),
			maxKey:  kv.Key("key2"),
			wantErr: false,
		},
		{
			name:    "same keys",
			minKey:  kv.Key("same"),
			maxKey:  kv.Key("same"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建 Header 并编码
			header := NewHeader(tt.minKey, tt.maxKey)
			buf := &bytes.Buffer{}
			err := header.EncodeTo(buf)

			if tt.wantErr {
				assert.Error(t, err, "EncodeTo should error")
				return
			}
			assert.NoError(t, err, "EncodeTo should not error")

			// 创建临时文件用于解码测试
			tmpFile, err := os.CreateTemp("", "header_test")
			assert.NoError(t, err, "CreateTemp should not error")
			defer os.Remove(tmpFile.Name())

			// 将编码后的数据写入临时文件
			_, err = tmpFile.Write(buf.Bytes())
			assert.NoError(t, err, "Write to temp file should not error")
			_, err = tmpFile.Seek(0, 0) // 重置文件指针
			assert.NoError(t, err, "Seek should not error")

			// 解码
			decodedHeader := NewHeader("", "nil")
			err = decodedHeader.DecodeFrom(tmpFile)

			if tt.wantErr {
				assert.Error(t, err, "DecodeFrom should error")
				return
			}
			assert.NoError(t, err, "DecodeFrom should not error")

			// 验证解码后的数据
			assert.Equal(t, tt.minKey, decodedHeader.MinKey, "MinKey mismatch")
			assert.Equal(t, tt.maxKey, decodedHeader.MaxKey, "MaxKey mismatch")
		})
	}
}

func TestHeader_DecodeFrom_FileErrors(t *testing.T) {
	// 测试文件读取错误的情况
	t.Run("file read error", func(t *testing.T) {
		// 创建一个不存在的文件路径
		nonExistentFile, err := os.Open("non_existent_file")
		assert.Error(t, err, "Open should error for non-existent file")
		if nonExistentFile != nil {
			defer nonExistentFile.Close()
		}

		header := NewHeader("key1", "key2")
		err = header.DecodeFrom(nonExistentFile)
		assert.Error(t, err, "DecodeFrom should error with invalid file")
	})

	// 测试文件数据不完整的情况
	t.Run("incomplete data", func(t *testing.T) {
		// 创建临时文件并写入不完整的数据（只写入 minKey）
		tmpFile, err := os.CreateTemp("", "incomplete_header_test")
		assert.NoError(t, err, "CreateTemp should not error")
		defer os.Remove(tmpFile.Name())

		minKey := kv.Key("key1")
		_, err = tmpFile.Write([]byte(minKey)) // 只写入 minKey，不写入 maxKey
		assert.NoError(t, err, "Write to temp file should not error")
		_, err = tmpFile.Seek(0, 0) // 重置文件指针
		assert.NoError(t, err, "Seek should not error")

		header := NewHeader("", "")
		err = header.DecodeFrom(tmpFile)
		assert.Error(t, err, "DecodeFrom should error with incomplete data")
	})
}

func TestHeader_EncodeTo_WriterErrors(t *testing.T) {
	// 测试写入错误的情况
	t.Run("writer error", func(t *testing.T) {
		// 创建一个会报错的 Writer
		errorWriter := &ErrorWriter{}
		header := NewHeader("key1", "key2")

		err := header.EncodeTo(errorWriter)
		assert.Error(t, err, "EncodeTo should error with faulty writer")
	})
}

// ErrorWriter 是一个会报错的 io.Writer 实现
type ErrorWriter struct{}

func (w *ErrorWriter) Write(p []byte) (int, error) {
	return 0, errors.New("mock write error")
}
