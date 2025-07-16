package sstable

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/sstable/block"
)

func setupTestEnv(t *testing.T) string {
	tempDir, err := os.MkdirTemp("", "sstable_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return tempDir
}

func cleanupTestEnv(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Logf("Warning: Failed to clean up temp dir: %v", err)
	}
}

func createSampleSSTable(level int) *SSTable {
	table := NewSSTableWithLevel(level)

	// Add some sample data
	table.DataBlock.Entries = []kv.Value{
		kv.Value("value1"),
		kv.Value("value2"),
	}

	// Setup index block
	table.IndexBlock.Indexes = []*block.IndexEntry{
		{Key: "key1", Offset: 0},
		{Key: "key2", Offset: 100},
	}

	// Setup header
	table.Header = &block.Header{
		MinKey: "key1",
		MaxKey: "key2",
	}

	// Setup filter
	table.FilterBlock.Add([]byte(("key1")))
	table.FilterBlock.Add([]byte(("key2")))

	return table
}

func TestNewSSTable(t *testing.T) {
	table := NewSSTable()
	assert.NotZero(t, table.id)
	assert.NotNil(t, table.IndexBlock)
	assert.NotNil(t, table.FilterBlock)
	assert.NotNil(t, table.Footer)
	assert.NotNil(t, table.DataBlock)
}

func TestNewSSTableWithLevel(t *testing.T) {
	table := NewSSTableWithLevel(1)
	assert.Equal(t, 1, table.level)
	assert.NotEmpty(t, table.filePath)
}

func TestEncodeDecode(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	// Create and encode
	table := createSampleSSTable(0)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)
	assert.FileExists(t, table.filePath)

	// Decode
	newTable := NewRecoverSSTable(0)
	err = newTable.DecodeFrom(table.filePath)
	assert.NoError(t, err)

	// Verify decoded data
	assert.Equal(t, table.Header.MinKey, newTable.Header.MinKey)
	assert.Equal(t, table.Header.MaxKey, newTable.Header.MaxKey)
	assert.Equal(t, len(table.IndexBlock.Indexes), len(newTable.IndexBlock.Indexes))
}

func TestDecodeFooterFrom(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	table := createSampleSSTable(0)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	file, err := os.Open(table.filePath)
	assert.NoError(t, err)
	defer file.Close()

	newTable := NewRecoverSSTable(0)
	err = newTable.DecodeFooterFrom(file)
	assert.NoError(t, err)
	assert.NotZero(t, newTable.Footer.IndexHandle.Offset)
	assert.NotZero(t, newTable.Footer.IndexHandle.Size)
}

func TestDecodeDataBlock(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	table := createSampleSSTable(0)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	file, err := os.Open(table.filePath)
	assert.NoError(t, err)
	defer file.Close()

	newTable := NewRecoverSSTable(0)
	// First decode footer to get data block position
	err = newTable.DecodeFooterFrom(file)
	assert.NoError(t, err)

	// Reset file pointer
	_, err = file.Seek(0, 0)
	assert.NoError(t, err)

	// Decode data block
	err = newTable.DecodeDataBlock(file)
	assert.NoError(t, err)
	assert.Equal(t, len(table.DataBlock.Entries), len(newTable.DataBlock.Entries))
}

func TestGetDataBlockFromFile(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	table := createSampleSSTable(0)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	err = table.DecodeFrom(table.filePath)
	assert.NoError(t, err)
	table.DataBlock.Entries = make([]kv.Value, 0)

	pairs, err := table.GetDataBlockFromFile(table.filePath)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(pairs))
	assert.Equal(t, kv.Key("key1"), pairs[0].Key)
	assert.Equal(t, kv.Key("key2"), pairs[1].Key)
}

func TestGetKeyValuePairs(t *testing.T) {
	table := createSampleSSTable(0)

	// Test normal case
	pairs, err := table.GetKeyValuePairs()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(pairs))
	assert.Equal(t, kv.Key("key1"), pairs[0].Key)
	assert.Equal(t, kv.Key("key2"), pairs[1].Key)

	// Test mismatched lengths
	table.DataBlock.Entries = table.DataBlock.Entries[:1]
	_, err = table.GetKeyValuePairs()
	assert.Error(t, err)

	// Test empty case
	table.DataBlock.Entries = nil
	table.IndexBlock.Indexes = nil
	pairs, err = table.GetKeyValuePairs()
	assert.NoError(t, err)
	assert.Empty(t, pairs)
}

func TestGetValueByOffset(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	table := createSampleSSTable(0)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	value, err := table.GetValueByOffset(table.IndexBlock.Indexes[0].Offset)
	assert.NoError(t, err)
	assert.Equal(t, table.DataBlock.Entries[0], value)
}

func TestMayContain(t *testing.T) {
	table := createSampleSSTable(0)

	assert.True(t, table.MayContain(kv.Key("key1")))
	assert.True(t, table.MayContain(kv.Key("key2")))
	assert.False(t, table.MayContain(kv.Key("nonexistent")))
}

func TestIdAndFilePath(t *testing.T) {
	table := NewSSTableWithLevel(1)
	assert.NotZero(t, table.ID())
	assert.Contains(t, table.FilePath(), filepath.Join("1-level", strconv.FormatUint(table.ID(), 10)+".sst"))
}

func TestRemove(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	table := createSampleSSTable(0)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)
	assert.FileExists(t, table.filePath)

	err = table.Remove()
	assert.NoError(t, err)
	assert.NoFileExists(t, table.filePath)
}

func TestSSTableFilePath(t *testing.T) {
	path := sstableFilePath(123, 2, "/test/path")
	assert.Equal(t, filepath.Join("/test/path", "2-level", "123.sst"), path)
}

func TestSSTableLevelPath(t *testing.T) {
	path := sstableLevelPath(3, "/test/path")
	assert.Equal(t, filepath.Join("/test/path", "3-level"), path)
}

func TestEncodeDecode_EmptySSTable(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	table := createSampleSSTable(minSSTableLevel)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)
	assert.FileExists(t, table.filePath)

	newTable := NewRecoverSSTable(0)
	err = newTable.DecodeFrom(table.filePath)
	assert.NoError(t, err)

	assert.Equal(t, table.Header.MinKey, newTable.Header.MinKey)
	assert.Equal(t, table.Header.MaxKey, newTable.Header.MaxKey)
	assert.Empty(t, newTable.DataBlock.Entries)
}

func TestEncodeDecode_WithRealData(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	table := NewSSTableWithLevel(0)
	table.Header = &block.Header{MinKey: "key1", MaxKey: "key3"}

	// 添加真实数据
	table.DataBlock.Entries = []kv.Value{
		kv.Value("value1"),
		kv.Value("value2"),
		kv.Value("value3"),
	}

	table.IndexBlock.Indexes = []*block.IndexEntry{
		{Key: "key1", Offset: 0},
		{Key: "key2", Offset: 100},
		{Key: "key3", Offset: 200},
	}

	table.FilterBlock.Add([]byte("key1"))
	table.FilterBlock.Add([]byte("key2"))
	table.FilterBlock.Add([]byte("key3"))

	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	newTable := NewRecoverSSTable(0)
	err = newTable.DecodeFrom(table.filePath)
	assert.NoError(t, err)

	assert.Equal(t, table.Header.MinKey, newTable.Header.MinKey)
	assert.Equal(t, table.Header.MaxKey, newTable.Header.MaxKey)
	assert.Equal(t, len(table.IndexBlock.Indexes), len(newTable.IndexBlock.Indexes))
	assert.Equal(t, 0, len(newTable.DataBlock.Entries)) // DataBlock should be empty after decoding
}

func TestEncodeTo_DirectoryCreationFailed(t *testing.T) {
	// 使用不存在的根目录来模拟目录创建失败
	table := NewSSTableWithLevel(0)
	table.filePath = "/nonexistent/path/123.sst"

	err := table.EncodeTo(table.filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create directory failed")
}

func TestDecodeFrom_FileNotFound(t *testing.T) {
	table := NewRecoverSSTable(0)
	err := table.DecodeFrom("/nonexistent/file.sst")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open file error")
}

func TestDecodeFrom_CorruptedHeader(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	// 创建一个空文件模拟损坏的头
	filePath := filepath.Join(tempDir, "corrupted.sst")
	err := os.WriteFile(filePath, []byte("invalid data"), 0644)
	assert.NoError(t, err)

	table := NewRecoverSSTable(0)
	err = table.DecodeFrom(filePath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decode Header failed")
}

func TestGetKeyValuePairs_MismatchedLengths(t *testing.T) {
	table := createSampleSSTable(0)

	// 故意制造不匹配
	table.DataBlock.Entries = table.DataBlock.Entries[:1] // 只有1个entry
	// IndexBlock保持2个entry

	pairs, err := table.GetKeyValuePairs()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mismatched DataBlock and IndexBlock entries")
	assert.Empty(t, pairs)
}

func TestGetValueByOffset_InvalidOffset(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	table := createSampleSSTable(0)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	// 测试超出范围的偏移量
	_, err = table.GetValueByOffset(999999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decode value length")
}

func TestMayContain_EdgeCases(t *testing.T) {
	table := createSampleSSTable(0)
	table.Header.MinKey = "key1"
	table.Header.MaxKey = "key2"

	// 测试边界情况
	assert.False(t, table.MayContain(kv.Key("key0"))) // 小于MinKey
	assert.False(t, table.MayContain(kv.Key("key3"))) // 大于MaxKey
	assert.False(t, table.MayContain(kv.Key("")))     // 空key
}

func TestRemove_FileNotExist(t *testing.T) {
	table := NewSSTableWithLevel(0)
	table.filePath = "/nonexistent/file.sst"

	// 删除不存在的文件应该返回错误
	err := table.Remove()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no such file or directory")
}

func TestSSTableFilePath_InvalidLevel(t *testing.T) {
	// 测试负数的level
	path := sstableFilePath(123, -1, "/test/path")
	assert.Equal(t, filepath.Join("/test/path", "-1-level", "123.sst"), path)
}

func TestConcurrentAccess(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	table := createSampleSSTable(0)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	// 并发读取测试
	var results [5]error
	for i := 0; i < 5; i++ {
		go func(idx int) {
			newTable := NewRecoverSSTable(0)
			results[idx] = newTable.DecodeFrom(table.filePath)
		}(i)
	}

	// 等待所有goroutine完成
	for _, res := range results {
		assert.NoError(t, res)
	}
}
