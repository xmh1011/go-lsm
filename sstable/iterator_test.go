package sstable

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
	"github.com/xmh1011/go-lsm/sstable/bloom"
)

// createTestSSTable 构造一个 SSTable 实例用于测试
func createTestSSTable(t *testing.T) *SSTable {
	sst := NewSSTable()

	// 构造 DataBlock 1：包含键 "a", "b"
	db1 := &block.DataBlock{
		Entries: []kv.Value{
			[]byte("A"),
			[]byte("B"),
			[]byte("C"),
			[]byte("D"),
			[]byte("E"),
		},
	}

	sst.DataBlock = db1

	// 构造索引块（记录每个数据块的起始键和偏移量）
	sst.IndexBlock = &block.IndexBlock{
		Indexes: []*block.IndexEntry{
			{Key: "a", Offset: 0},
			{Key: "b", Offset: 0},
			{Key: "c", Offset: 0},
			{Key: "d", Offset: 0},
			{Key: "e", Offset: 0},
		},
	}

	// 构造布隆过滤器
	sst.FilterBlock = bloom.NewBloomFilter(1024, 5)
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		sst.FilterBlock.AddString(key)
	}

	return sst
}

func TestSSTableIterator(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "1.sst")

	// 创建并写入SSTable
	original := createTestSSTable(t)
	err := original.EncodeTo(filePath)
	assert.NoError(t, err)
	defer original.Remove()

	// 重新加载SSTable进行测试
	loaded := NewSSTable()
	err = loaded.DecodeFrom(filePath)
	assert.NoError(t, err)
	defer loaded.Remove()

	t.Run("SequentialTraversal", func(t *testing.T) {
		iter := NewSSTableIterator(loaded)
		defer iter.Close()

		var keys, values []string
		for iter.Valid() {
			keys = append(keys, string(iter.Key()))
			val, err := iter.Value()
			assert.NoError(t, err)
			values = append(values, string(val))
			iter.Next()
		}

		// 验证遍历顺序和内容
		assert.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)
		assert.Equal(t, []string{"A", "B", "C", "D", "E"}, values)
	})

	t.Run("SeekOperations", func(t *testing.T) {
		tests := []struct {
			name        string
			seekKey     kv.Key
			expectKey   kv.Key
			expectVal   string
			expectValid bool
		}{
			{
				name:        "SeekExactMatch",
				seekKey:     "b",
				expectKey:   "b",
				expectVal:   "B",
				expectValid: true,
			},
			{
				name:        "SeekNonExistKey",
				seekKey:     "bb",
				expectKey:   "c",
				expectVal:   "C",
				expectValid: false,
			},
			{
				name:        "SeekFirstKey",
				seekKey:     "a",
				expectKey:   "a",
				expectVal:   "A",
				expectValid: true,
			},
			{
				name:        "SeekLastKey",
				seekKey:     "e",
				expectKey:   "e",
				expectVal:   "E",
				expectValid: true,
			},
			{
				name:        "SeekBeyondLast",
				seekKey:     "z",
				expectKey:   "",
				expectVal:   "",
				expectValid: false,
			},
			{
				name:        "SeekBeforeFirst",
				seekKey:     "aa",
				expectKey:   "",
				expectVal:   "",
				expectValid: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				iter := NewSSTableIterator(loaded)
				defer iter.Close()

				iter.Seek(tt.seekKey)
				assert.Equal(t, tt.expectValid, iter.Valid())

				if tt.expectValid {
					assert.Equal(t, tt.expectKey, iter.Key())
					val, err := iter.Value()
					assert.NoError(t, err)
					assert.Equal(t, tt.expectVal, string(val))
				}
			})
		}
	})

	t.Run("PositioningMethods", func(t *testing.T) {
		iter := NewSSTableIterator(loaded)
		defer iter.Close()

		// 测试SeekToFirst
		iter.SeekToFirst()
		assert.True(t, iter.Valid())
		assert.Equal(t, "a", string(iter.Key()))

		// 测试SeekToLast
		iter.SeekToLast()
		assert.True(t, iter.Valid())
		assert.Equal(t, "e", string(iter.Key()))
	})
}

func TestFilterBlockIntegration(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "1.sst")

	original := createTestSSTable(t)
	err := original.EncodeTo(filePath)
	assert.NoError(t, err)

	loaded := NewSSTable()
	err = loaded.DecodeFrom(filePath)
	assert.NoError(t, err)

	// 测试存在的键
	assert.True(t, loaded.FilterBlock.MayContain("a"))
	assert.True(t, loaded.FilterBlock.MayContain("e"))

	// 测试不存在的键
	assert.False(t, loaded.FilterBlock.MayContain("x"))
}
