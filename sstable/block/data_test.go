package block

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
)

func buildTestDataBlock() *DataBlock {
	return &DataBlock{
		Records: []*kv.KeyValuePair{
			{Key: "apple", Value: []byte("fruit")},
			{Key: "banana", Value: []byte("yellow")},
			{Key: "carrot", Value: []byte("vegetable")},
			{Key: "date", Value: []byte("sweet")},
		},
	}
}

func TestDataBlockEncodeDecode(t *testing.T) {
	// 构造原始数据
	original := buildTestDataBlock()

	// 创建临时文件作为 WriteSeeker 和 ReadSeeker
	dir := t.TempDir()
	path := filepath.Join(dir, "datablock.data")
	file, err := os.Create(path)
	assert.NoError(t, err)
	defer file.Close()

	// 编码 DataBlock
	handle, err := original.EncodeTo(file)
	assert.NoError(t, err)

	// 准备 Handle 并构造读取对象
	readFile, err := os.Open(path)
	assert.NoError(t, err)
	defer readFile.Close()

	// 解码 DataBlock
	decoded := NewDataBlock()
	err = decoded.DecodeFrom(readFile, handle)
	assert.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestIteratorSeekToFirstAndNext(t *testing.T) {
	block := buildTestDataBlock()
	it := NewDataBlockIterator(block)

	it.SeekToFirst()
	assert.True(t, it.Valid())
	assert.Equal(t, kv.Key("apple"), it.Key())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, kv.Key("banana"), it.Key())

	it.Next()
	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, kv.Key("date"), it.Key())

	it.Next()
	assert.False(t, it.Valid())
}

func TestIteratorSeekToLast(t *testing.T) {
	block := buildTestDataBlock()
	it := NewDataBlockIterator(block)

	it.SeekToLast()
	assert.True(t, it.Valid())
	assert.Equal(t, kv.Key("date"), it.Key())
}

func TestIteratorSeekExactAndGreater(t *testing.T) {
	block := buildTestDataBlock()
	it := NewDataBlockIterator(block)

	it.Seek("banana")
	assert.True(t, it.Valid())
	assert.Equal(t, kv.Key("banana"), it.Key())

	it.Seek("blueberry") // 不存在，应跳到 "carrot"
	assert.True(t, it.Valid())
	assert.Equal(t, kv.Key("carrot"), it.Key())

	it.Seek("zzz") // 超出最大 key，应无效
	assert.False(t, it.Valid())
}

func TestIteratorEmptyBlock(t *testing.T) {
	block := &DataBlock{}
	it := NewDataBlockIterator(block)

	it.SeekToFirst()
	assert.False(t, it.Valid())

	it.SeekToLast()
	assert.False(t, it.Valid())

	it.Seek("any")
	assert.False(t, it.Valid())
}
