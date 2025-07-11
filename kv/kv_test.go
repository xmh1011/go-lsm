package kv

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyValuePair_EncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		pair     *KeyValuePair
		wantErr  bool
		checkVal bool // 是否检查Value是否正确解码
	}{
		{
			name: "normal key-value pair",
			pair: &KeyValuePair{
				Key:   "test_key",
				Value: []byte("test_value"),
			},
			wantErr:  false,
			checkVal: true,
		},
		{
			name: "empty key",
			pair: &KeyValuePair{
				Key:   "",
				Value: []byte("value_only"),
			},
			wantErr:  false,
			checkVal: true,
		},
		{
			name: "empty value",
			pair: &KeyValuePair{
				Key:   "key_only",
				Value: []byte{},
			},
			wantErr:  false,
			checkVal: true,
		},
		{
			name: "deleted value",
			pair: &KeyValuePair{
				Key:   "deleted_key",
				Value: DeletedValue,
			},
			wantErr:  false,
			checkVal: true,
		},
		{
			name: "large key and value",
			pair: &KeyValuePair{
				Key:   Key("large_key_" + string(make([]byte, 1000))),
				Value: make([]byte, 2000),
			},
			wantErr:  false,
			checkVal: false, // 大值不检查具体内容，只检查长度
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 编码
			buf := &bytes.Buffer{}
			err := tt.pair.EncodeTo(buf)

			if tt.name == "invalid key length" {
				// 手动破坏数据来模拟错误
				buf = bytes.NewBuffer([]byte{0xFF, 0xFF, 0xFF, 0xFF}) // 超大长度前缀
			}

			if tt.wantErr {
				assert.Error(t, err, "EncodeTo() should return error")
				return
			}

			assert.NoError(t, err, "EncodeTo() should not return error")

			// 解码
			decoded := &KeyValuePair{}
			err = decoded.DecodeFrom(buf)
			assert.NoError(t, err, "DecodeFrom() should not return error")

			// 验证
			assert.Equal(t, tt.pair.Key, decoded.Key, "Key should match")

			if tt.checkVal {
				assert.Equal(t, tt.pair.Value, decoded.Value, "Value should match")
			} else {
				assert.Equal(t, len(tt.pair.Value), len(decoded.Value), "Value length should match")
			}

			// 验证IsDeleted方法
			if tt.name == "deleted value" {
				assert.True(t, decoded.IsDeleted(), "IsDeleted() should return true for deleted value")
			} else {
				assert.False(t, decoded.IsDeleted(), "IsDeleted() should return false for non-deleted value")
			}
		})
	}
}

func TestKeyValuePair_Copy(t *testing.T) {
	original := &KeyValuePair{
		Key:   "test_key",
		Value: []byte("test_value"),
	}

	copied := original.Copy()

	// 验证拷贝后的对象是否相等
	assert.Equal(t, original.Key, copied.Key, "Key should be equal")
	assert.Equal(t, original.Value, copied.Value, "Value should be equal")

	// 验证修改拷贝不会影响原始对象
	copied.Key = "modified_key"
	copied.Value = []byte("modified_value")

	assert.NotEqual(t, original.Key, copied.Key, "Original key should not be modified")
	assert.NotEqual(t, original.Value, copied.Value, "Original value should not be modified")
}

func TestKey_EncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		key     Key
		wantErr bool
	}{
		{
			name:    "normal key",
			key:     "test_key",
			wantErr: false,
		},
		{
			name:    "empty key",
			key:     "",
			wantErr: false,
		},
		{
			name:    "long key",
			key:     Key(make([]byte, 1000)),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 编码
			buf := &bytes.Buffer{}
			n, err := tt.key.EncodeTo(buf)
			assert.NoError(t, err, "EncodeTo() should not return error")
			assert.True(t, n > 0, "EncodeTo() should return positive byte count")

			// 解码
			var decoded Key
			readBytes, err := decoded.DecodeFrom(buf)
			assert.NoError(t, err, "DecodeFrom() should not return error")
			assert.True(t, readBytes > 0, "DecodeFrom() should return positive byte count")

			// 验证
			assert.Equal(t, tt.key, decoded, "Decoded key should match original")
		})
	}
}

func TestValue_EncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		value   Value
		wantErr bool
	}{
		{
			name:    "normal value",
			value:   []byte("test_value"),
			wantErr: false,
		},
		{
			name:    "empty value",
			value:   []byte{},
			wantErr: false,
		},
		{
			name:    "deleted value",
			value:   DeletedValue,
			wantErr: false,
		},
		{
			name:    "large value",
			value:   make([]byte, 2000),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 编码
			buf := &bytes.Buffer{}
			n, err := tt.value.EncodeTo(buf)
			assert.NoError(t, err, "EncodeTo() should not return error")
			assert.True(t, n > 0, "EncodeTo() should return positive byte count")

			// 解码
			var decoded Value
			err = decoded.DecodeFrom(buf)
			assert.NoError(t, err, "DecodeFrom() should not return error")

			// 验证
			if tt.name != "large value" { // 大值不检查具体内容
				assert.Equal(t, tt.value, decoded, "Decoded value should match original")
			} else {
				assert.Equal(t, len(tt.value), len(decoded), "Decoded value length should match")
			}

			// 验证IsDeleted方法
			if tt.name == "deleted value" {
				pair := &KeyValuePair{Value: decoded}
				assert.True(t, pair.IsDeleted(), "IsDeleted() should return true for deleted value")
			}
		})
	}
}
