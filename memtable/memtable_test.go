package memtable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/kv"
)

// TestNewMemTable tests that a new MemTable is created with a valid WAL.
func TestNewMemTable(t *testing.T) {
	// Use t.TempDir() for an isolated temporary directory.
	tempDir := t.TempDir()
	m := NewMemTable(1, tempDir)
	assert.NotNil(t, m, "MemTable should not be nil")
	assert.Equal(t, uint64(1), m.id, "MemTable ID should match")

	// Check that the WAL file is created.
	walPath := filepath.Join(tempDir, "1.wal")
	_, err := os.Stat(walPath)
	assert.NoError(t, err, "WAL file should exist at %s", walPath)
}

// TestInsertAndSearch verifies that inserting a key/value pair and then searching for it works as expected.
func TestInsertAndSearch(t *testing.T) {
	tempDir := t.TempDir()
	m := NewMemTable(2, tempDir)
	pair := kv.KeyValuePair{
		Key:   "testKey",
		Value: []byte("testValue"),
	}

	err := m.Insert(pair)
	assert.NoError(t, err, "Insert should succeed")

	val, found := m.Search("testKey")
	assert.True(t, found, "Seek should find the inserted key")
	assert.Equal(t, pair.Value, val, "The value should match")
}

// TestDelete ensures that deleting a key works properly.
func TestDelete(t *testing.T) {
	tempDir := t.TempDir()
	m := NewMemTable(3, tempDir)
	pair := kv.KeyValuePair{
		Key:   "delKey",
		Value: kv.DeletedValue,
	}

	// Insert the key/value pair.
	err := m.Insert(pair)
	assert.NoError(t, err, "Insert should succeed")

	// Delete the key.
	err = m.Delete("delKey")
	assert.NoError(t, err, "Delete should succeed")

	// delete key not exist.
	err = m.Delete("nonexistent")
	assert.NoError(t, err)

	// Seek for the key after deletion.
	_, found := m.Search("delKey")
	assert.False(t, found, "Deleted key should not be found")
}

// TestCanInsertAndApproximateSize 测试容量判断和大小统计
func TestCanInsertAndApproximateSize(t *testing.T) {
	m := NewMemTableWithoutWAL()
	pair := kv.KeyValuePair{
		Key:   "key",
		Value: []byte("value"),
	}
	initialSize := m.ApproximateSize()
	assert.True(t, m.CanInsert(pair))
	_ = m.Insert(pair)
	assert.True(t, m.ApproximateSize() > initialSize)
}

// TestRecoverFromWAL 测试正常恢复过程（这里用实际文件或模拟依赖较复杂，只做简单接口层测试）
func TestRecoverFromWAL(t *testing.T) {
	// 先创建一个 MemTable 并插入数据，然后 Close WAL，准备恢复
	tmp := t.TempDir()
	config.Conf.WALPath = tmp
	m := NewMemTable(100, config.GetWALPath())
	pair := kv.KeyValuePair{Key: "recoverKey", Value: []byte("recoverValue")}
	err := m.Insert(pair)
	assert.NoError(t, err)

	// 创建一个新实例来恢复
	m2 := NewMemTable(100, config.GetWALPath())
	err = m2.RecoverFromWAL("100.wal")
	assert.NoError(t, err)

	val, found := m2.Search("recoverKey")
	assert.True(t, found)
	assert.Equal(t, kv.Value("recoverValue"), val)
}
