package skiplist

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
)

// TestSkipListAddAndSearch tests the basic Add and Seek methods of the SkipList.
func TestSkipListAddAndSearch(t *testing.T) {
	sl := NewSkipList()

	// Define test key/value pairs.
	pairs := []kv.KeyValuePair{
		{Key: "apple", Value: []byte("fruit")},
		{Key: "banana", Value: []byte("yellow")},
		{Key: "cherry", Value: []byte("red")},
	}

	// Insert each pair into the skip list.
	for _, pair := range pairs {
		sl.Add(pair)
	}

	// Seek for each inserted key and verify the values.
	for _, pair := range pairs {
		value, found := sl.Search(pair.Key)
		assert.True(t, found, "expected to find key %s", pair.Key)
		assert.Equal(t, pair.Value, value, "expected value %s for key %s", string(pair.Value), pair.Key)
	}

	// Ensure that a non-existent key returns not found.
	_, found := sl.Search("non-existent")
	assert.False(t, found, "expected key 'non-existent' to not be found")
}

// TestSkipListDelete tests the Delete method.
func TestSkipListDelete(t *testing.T) {
	sl := NewSkipList()
	pairs := []kv.KeyValuePair{
		{Key: "alpha", Value: []byte("first")},
		{Key: "beta", Value: []byte("second")},
		{Key: "gamma", Value: []byte("third")},
	}
	// Insert the key/value pairs.
	for _, pair := range pairs {
		sl.Add(pair)
	}

	// Delete key "beta" and verify deletion.
	deleted := sl.Delete("beta")
	assert.True(t, deleted, "expected to delete key 'beta'")
	_, found := sl.Search("beta")
	assert.False(t, found, "expected key 'beta' to not be found after deletion")

	// Verify that the other keys still exist.
	for _, k := range []kv.Key{"alpha", "gamma"} {
		_, found := sl.Search(k)
		assert.True(t, found, "expected to find key %s", k)
	}
}
