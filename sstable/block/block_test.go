package block

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandleEncodeDecode(t *testing.T) {
	// 构造原始 Handle
	original := Handle{
		Offset: 12345,
		Size:   6789,
	}

	// 编码到 bytes.Buffer
	buf := new(bytes.Buffer)
	err := original.EncodeTo(buf)
	assert.NoError(t, err)

	// 解码回新的 Handle
	var decoded Handle
	err = decoded.DecodeFrom(buf)
	assert.NoError(t, err)

	// 比较原始和解码结果
	assert.Equal(t, original, decoded)
}
