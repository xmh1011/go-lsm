package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
)

func TestNewSSTableBuilder(t *testing.T) {
	builder := NewSSTableBuilder(1)
	assert.NotNil(t, builder)
	assert.NotNil(t, builder.table)
	assert.Equal(t, uint64(0), builder.size)
	assert.Equal(t, 1, builder.table.level)
}

func TestBuilder_Add(t *testing.T) {
	builder := NewSSTableBuilder(0)

	// 添加测试数据
	pair1 := &kv.KeyValuePair{
		Key:   "key1",
		Value: kv.Value("value1"),
	}
	pair2 := &kv.KeyValuePair{
		Key:   "key2",
		Value: kv.Value("value2"),
	}

	builder.Add(pair1)
	builder.Add(pair2)

	// 验证 DataBlock
	assert.Equal(t, 2, builder.table.DataBlock.Len())

	// 验证 IndexBlock
	assert.Equal(t, 2, len(builder.table.IndexBlock.Indexes))
	assert.Equal(t, kv.Key("key1"), builder.table.IndexBlock.Indexes[0].Key)
	assert.Equal(t, kv.Key("key2"), builder.table.IndexBlock.Indexes[1].Key)

	// 验证 size 计算
	expectedSize := pair1.EstimateSize() + pair2.EstimateSize()
	assert.Equal(t, expectedSize, builder.size)
}

func TestBuilder_ShouldFlush(t *testing.T) {
	tests := []struct {
		name     string
		size     uint64
		expected bool
	}{
		{
			name:     "not flush",
			size:     maxSSTableSize - 1,
			expected: false,
		},
		{
			name:     "exact flush",
			size:     maxSSTableSize,
			expected: true,
		},
		{
			name:     "exceed flush",
			size:     maxSSTableSize + 1,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewSSTableBuilder(0)
			builder.size = tt.size
			assert.Equal(t, tt.expected, builder.ShouldFlush())
		})
	}
}

func TestBuilder_Finalize(t *testing.T) {
	builder := NewSSTableBuilder(0)

	// 添加一些数据
	builder.table.DataBlock.Add(kv.Value("value1"))
	builder.table.DataBlock.Add(kv.Value("value2"))
	builder.table.IndexBlock.Add("key1", 0)
	builder.table.IndexBlock.Add("key2", 100)

	builder.Finalize()

	// 验证 Header 是否正确设置
	assert.NotNil(t, builder.table.Header)
	assert.Equal(t, kv.Key("key1"), builder.table.Header.MinKey)
	assert.Equal(t, kv.Key("key2"), builder.table.Header.MaxKey)
}

func TestBuilder_Build(t *testing.T) {
	builder := NewSSTableBuilder(0)

	// 添加一些数据
	pair := &kv.KeyValuePair{
		Key:   "testKey",
		Value: kv.Value("testValue"),
	}
	builder.Add(pair)

	sstable := builder.Build()

	// 验证返回的 SSTable
	assert.NotNil(t, sstable)
	assert.Equal(t, builder.table, sstable)

	// 验证 Finalize 已经被调用
	assert.NotNil(t, sstable.Header)
	assert.Equal(t, kv.Key("testKey"), sstable.Header.MinKey)
	assert.Equal(t, kv.Key("testKey"), sstable.Header.MaxKey)
}
