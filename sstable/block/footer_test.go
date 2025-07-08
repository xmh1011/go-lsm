package block

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandle_EncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		handle  Handle
		wantErr bool
	}{
		{
			name:    "zero handle",
			handle:  NewHandle(0, 0),
			wantErr: false,
		},
		{
			name:    "non-zero handle",
			handle:  NewHandle(1234, 5678),
			wantErr: false,
		},
		{
			name:    "max offset and size",
			handle:  NewHandle(^int64(0), ^int64(0)), // 最大int64值
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 编码
			buf := &bytes.Buffer{}
			err := tt.handle.EncodeTo(buf)
			assert.Equal(t, tt.wantErr, err != nil, "EncodeTo error mismatch")
			if tt.wantErr {
				return
			}

			// 验证编码后的数据长度
			assert.Equal(t, HandleSize, buf.Len(), "Encoded handle size mismatch")

			// 解码
			decodedHandle := NewHandle(0, 0)
			reader := bytes.NewReader(buf.Bytes())
			err = decodedHandle.DecodeFrom(reader)
			assert.NoError(t, err, "DecodeFrom should not error")

			// 验证解码结果
			assert.Equal(t, tt.handle.Offset, decodedHandle.Offset, "Offset mismatch")
			assert.Equal(t, tt.handle.Size, decodedHandle.Size, "Size mismatch")
		})
	}
}

func TestHandle_DecodeErrors(t *testing.T) {
	// 测试不完整的数据
	t.Run("incomplete data", func(t *testing.T) {
		// 只提供部分数据（HandleSize-1字节）
		incompleteData := make([]byte, HandleSize-1)
		reader := bytes.NewReader(incompleteData)

		handle := NewHandle(0, 0)
		err := handle.DecodeFrom(reader)
		assert.Error(t, err, "DecodeFrom should error with incomplete data")
	})
}

func TestFooter_EncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		footer  *Footer
		wantErr bool
	}{
		{
			name:    "zero footer",
			footer:  NewFooter(),
			wantErr: false,
		},
		{
			name: "non-zero footer",
			footer: &Footer{
				DataHandle:  NewHandle(100, 200),
				IndexHandle: NewHandle(300, 400),
			},
			wantErr: false,
		},
		{
			name: "max offset and size",
			footer: &Footer{
				DataHandle:  NewHandle(^int64(0), ^int64(0)),
				IndexHandle: NewHandle(^int64(0), ^int64(0)),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 编码
			buf := &bytes.Buffer{}
			err := tt.footer.EncodeTo(buf)
			assert.Equal(t, tt.wantErr, err != nil, "EncodeTo error mismatch")
			if tt.wantErr {
				return
			}

			// 验证编码后的数据长度
			assert.Equal(t, FooterSize, buf.Len(), "Encoded footer size mismatch")

			// 解码
			decodedFooter := NewFooter()
			reader := bytes.NewReader(buf.Bytes())
			err = decodedFooter.DecodeFrom(reader)
			assert.NoError(t, err, "DecodeFrom should not error")

			// 验证解码结果
			assert.Equal(t, tt.footer.DataHandle.Offset, decodedFooter.DataHandle.Offset, "DataHandle Offset mismatch")
			assert.Equal(t, tt.footer.DataHandle.Size, decodedFooter.DataHandle.Size, "DataHandle Size mismatch")
			assert.Equal(t, tt.footer.IndexHandle.Offset, decodedFooter.IndexHandle.Offset, "IndexHandle Offset mismatch")
			assert.Equal(t, tt.footer.IndexHandle.Size, decodedFooter.IndexHandle.Size, "IndexHandle Size mismatch")
		})
	}
}

func TestFooter_DecodeErrors(t *testing.T) {
	// 测试不完整的数据（只提供部分数据）
	t.Run("incomplete data", func(t *testing.T) {
		// 只提供部分数据（FooterSize-1字节）
		incompleteData := make([]byte, FooterSize-1)
		reader := bytes.NewReader(incompleteData)

		footer := NewFooter()
		err := footer.DecodeFrom(reader)
		assert.Error(t, err, "DecodeFrom should error with incomplete data")
	})

	// 测试损坏的Handle数据（第一个Handle正确，第二个Handle不完整）
	t.Run("corrupted second handle", func(t *testing.T) {
		// 先正确编码第一个Handle
		buf := &bytes.Buffer{}
		validHandle := NewHandle(100, 200)
		err := validHandle.EncodeTo(buf)
		assert.NoError(t, err)

		// 添加不完整的第二个Handle数据（HandleSize-1字节）
		buf.Write(make([]byte, HandleSize-1))

		footer := NewFooter()
		reader := bytes.NewReader(buf.Bytes())
		err = footer.DecodeFrom(reader)
		assert.Error(t, err, "DecodeFrom should error with corrupted second handle")
	})
}

func TestFooter_NewHandle(t *testing.T) {
	offset, size := int64(123), int64(456)
	handle := NewHandle(offset, size)

	assert.Equal(t, offset, handle.Offset, "Offset mismatch")
	assert.Equal(t, size, handle.Size, "Size mismatch")
}

func TestFooter_NewFooter(t *testing.T) {
	footer := NewFooter()

	assert.Equal(t, int64(0), footer.DataHandle.Offset, "DataHandle Offset should be 0")
	assert.Equal(t, int64(0), footer.DataHandle.Size, "DataHandle Size should be 0")
	assert.Equal(t, int64(0), footer.IndexHandle.Offset, "IndexHandle Offset should be 0")
	assert.Equal(t, int64(0), footer.IndexHandle.Size, "IndexHandle Size should be 0")
}
