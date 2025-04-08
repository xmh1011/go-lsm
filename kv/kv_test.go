package kv

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyValuePairEncodeToAndDecodeFrom(t *testing.T) {
	tests := []KeyValuePair{
		{Key: "name", Value: []byte("alice"), Deleted: false},
		{Key: "empty", Value: []byte{}, Deleted: false},
		{Key: "", Value: []byte("value-only"), Deleted: false},
		{Key: "to-delete", Value: []byte("some-value"), Deleted: true},
	}

	for _, input := range tests {
		var buf bytes.Buffer

		// 使用方法 EncodeTo
		err := input.EncodeTo(&buf)
		assert.NoError(t, err, "encoding should not fail")

		// 使用方法 DecodeFrom
		var decoded KeyValuePair
		err = decoded.DecodeFrom(&buf)
		assert.NoError(t, err, "decoding should not fail")

		assert.Equal(t, input.Key, decoded.Key, "keys should match")
		assert.Equal(t, input.Value, decoded.Value, "values should match")
		assert.Equal(t, input.Deleted, decoded.Deleted, "deleted flags should match")
	}
}

func TestKeyEncodeDecode(t *testing.T) {
	tests := []Key{
		"hello",
		"",
		"你好，世界",
		"key-with-🚀-unicode",
		Key(make([]byte, 1024)), // long key
	}

	for _, original := range tests {
		var buf bytes.Buffer

		// 编码
		err := original.EncodeTo(&buf)
		assert.NoError(t, err, "encoding should not fail for key: %q", original)

		// 解码
		var decoded Key
		err = decoded.DecodeFrom(&buf)
		assert.NoError(t, err, "decoding should not fail for key: %q", original)

		// 比较结果
		assert.Equal(t, original, decoded, "original and decoded keys should be equal")
	}
}
