package block

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func buildTestIndexBlock() *IndexBlock {
	return &IndexBlock{
		StartKey: "a",
		Indexes: []*IndexEntry{
			{SeparatorKey: "apple", Handle: Handle{Offset: 100, Size: 20}},
			{SeparatorKey: "banana", Handle: Handle{Offset: 120, Size: 20}},
			{SeparatorKey: "carrot", Handle: Handle{Offset: 140, Size: 20}},
			{SeparatorKey: "date", Handle: Handle{Offset: 160, Size: 20}},
		},
	}
}

func TestIndexBlockEncodeDecode(t *testing.T) {
	original := buildTestIndexBlock()

	dir := t.TempDir()
	path := filepath.Join(dir, "indexblock.data")
	file, err := os.Create(path)
	assert.NoError(t, err)
	defer file.Close()

	handle, err := original.EncodeTo(file)
	assert.NoError(t, err)

	// 重新打开文件用于读取
	readFile, err := os.Open(path)
	assert.NoError(t, err)
	defer readFile.Close()

	decoded := NewIndexBlock()
	err = decoded.DecodeFrom(readFile, handle)
	assert.NoError(t, err)

	// 4. 验证解码内容
	assert.Equal(t, original.Indexes, decoded.Indexes)
}
