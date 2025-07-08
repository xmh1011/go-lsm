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
