package memtable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
)

// TestNewMemtable tests that a new Memtable is created with a valid WAL.
func TestNewMemtable(t *testing.T) {
	// Use t.TempDir() for an isolated temporary directory.
	tempDir := t.TempDir()
	m := NewMemtable(1, tempDir)
	assert.NotNil(t, m, "Memtable should not be nil")
	assert.Equal(t, uint64(1), m.id, "Memtable ID should match")

	// Check that the WAL file is created.
	walPath := filepath.Join(tempDir, "1.wal")
	_, err := os.Stat(walPath)
	assert.NoError(t, err, "WAL file should exist at %s", walPath)
}

// TestInsertAndSearch verifies that inserting a key/value pair and then searching for it works as expected.
func TestInsertAndSearch(t *testing.T) {
	tempDir := t.TempDir()
	m := NewMemtable(2, tempDir)
	pair := kv.KeyValuePair{
		Key:     "testKey",
		Value:   []byte("testValue"),
		Deleted: false,
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
	m := NewMemtable(3, tempDir)
	pair := kv.KeyValuePair{
		Key:     "delKey",
		Value:   []byte("delValue"),
		Deleted: false,
	}

	// Insert the key/value pair.
	err := m.Insert(pair)
	assert.NoError(t, err, "Insert should succeed")

	// Delete the key.
	err = m.Delete("delKey")
	assert.NoError(t, err, "Delete should succeed")

	// Seek for the key after deletion.
	_, found := m.Search("delKey")
	assert.False(t, found, "Deleted key should not be found")
}
