package skiplist

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
)

// TestSkipListIterator tests the iterator functionality of the SkipList.
func TestSkipListIterator(t *testing.T) {
	sl := NewSkipList()
	// Insert keys in unsorted order.
	pairs := []kv.KeyValuePair{
		{Key: "d", Value: []byte("4")},
		{Key: "a", Value: []byte("1")},
		{Key: "c", Value: []byte("3")},
		{Key: "b", Value: []byte("2")},
	}
	for _, pair := range pairs {
		sl.Add(pair)
	}

	// Expected order is sorted by key: a, b, c, d.
	expectedKeys := []kv.Key{"a", "b", "c", "d"}

	// Obtain an iterator from the skip list.
	iter := NewSkipListIterator(sl)
	// Calling SeekToFirst to ensure the iterator is at the first valid node.
	iter.SeekToFirst()

	var resultKeys []kv.Key
	for iter.Valid() {
		resultKeys = append(resultKeys, iter.Key())
		iter.Next()
	}
	iter.Close()

	assert.Equal(t, resultKeys, expectedKeys)
}

// TestSkipListIteratorSkipDeleted ensures that the iterator skips nodes marked as deleted.
func TestSkipListIteratorSkipDeleted(t *testing.T) {
	sl := NewSkipList()
	pairs := []kv.KeyValuePair{
		{Key: "x", Value: []byte("1")},
		{Key: "y", Value: []byte("2")},
		{Key: "z", Value: []byte("3")},
	}
	for _, pair := range pairs {
		sl.Add(pair)
	}
	// Delete key "y".
	sl.Delete("y")

	// Expect iterator to return only keys "x" and "z".
	expectedKeys := []kv.Key{"x", "z"}

	iter := NewSkipListIterator(sl)
	iter.SeekToFirst()
	var resultKeys []kv.Key
	for iter.Valid() {
		resultKeys = append(resultKeys, iter.Key())
		iter.Next()
	}
	iter.Close()

	assert.Equal(t, resultKeys, expectedKeys)
}
