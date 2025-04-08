package block

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterBlockEncodeDecode(t *testing.T) {
	// 创建临时文件作为 WriteSeeker 和 ReadSeeker
	dir := t.TempDir()
	path := filepath.Join(dir, "filterblock.data")
	file, err := os.Create(path)
	assert.NoError(t, err)
	defer file.Close()

	// 构造 FilterBlock
	original := NewFilterBlock(1024, 5)
	original.Filter.AddString("apple")
	original.Filter.AddString("banana")

	// 执行 EncodeTo
	handle, err := original.EncodeTo(file)
	assert.NoError(t, err)

	// 重新打开文件用于读取
	readFile, err := os.Open(path)
	assert.NoError(t, err)
	defer readFile.Close()

	// 执行 DecodeFrom
	decoded := FilterBlock{}
	err = decoded.DecodeFrom(readFile, handle)
	assert.NoError(t, err)

	// 验证过滤器功能
	assert.True(t, decoded.MayContain("apple"), "should contain 'apple'")
	assert.True(t, decoded.MayContain("banana"), "should contain 'banana'")
	assert.False(t, decoded.MayContain("orange"), "should not contain 'orange'")

	// 验证 MetaIndex（已写入文件）是否正确恢复（注意 original.MetaIndex 是旧值）
	assert.True(t, decoded.MetaIndex.Size > 0)
	assert.True(t, decoded.MetaIndex.Offset > 0)
}
