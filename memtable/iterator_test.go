package memtable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
)

// TestIterator verifies that the MemTable iterator returns keys in sorted order.
func TestIterator(t *testing.T) {
	tempDir := t.TempDir()
	m := NewMemTable(4, tempDir)

	// Insert several key/value pairs with unsorted keys.
	pairs := []kv.KeyValuePair{
		{Key: "banana", Value: []byte("yellow")},
		{Key: "apple", Value: []byte("red")},
		{Key: "cherry", Value: []byte("dark red")},
	}

	for _, pair := range pairs {
		err := m.Insert(pair)
		assert.NoError(t, err, "Insert should succeed")
	}

	// The expected order is sorted by key: apple, banana, cherry.
	expectedOrder := []kv.Key{"apple", "banana", "cherry"}

	iter := NewMemTableIterator(m.entries)
	var resultKeys []kv.Key
	for iter.Valid() {
		resultKeys = append(resultKeys, iter.Key())
		iter.Next()
	}
	iter.Close()

	assert.Equal(t, expectedOrder, resultKeys, "Iterator should return keys in sorted order")
}
