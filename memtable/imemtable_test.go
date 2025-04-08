package memtable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
)

// TestIMemtable_Search verifies that Seek works correctly for immutable memtables.
func TestIMemtableSearch(t *testing.T) {
	dir := t.TempDir()
	mem := NewMemtable(10, dir)

	err := mem.Insert(kv.KeyValuePair{Key: "alpha", Value: []byte("a")})
	assert.NoError(t, err)
	err = mem.Insert(kv.KeyValuePair{Key: "beta", Value: []byte("b")})
	assert.NoError(t, err)
	err = mem.Insert(kv.KeyValuePair{Key: "gamma", Value: []byte("g")})
	assert.NoError(t, err)

	// Freeze memtable to immutable
	imem := NewIMemtable(mem)

	// Seek for existing key
	val, found := imem.Search("beta")
	assert.True(t, found)
	assert.Equal(t, kv.Value("b"), val)

	// Seek for non-existent key
	_, found = imem.Search("delta")
	assert.False(t, found)
}

// TestIMemtableRangeScan verifies that RangeScan returns all keys in sorted order.
func TestIMemtableRangeScan(t *testing.T) {
	dir := t.TempDir()
	mem := NewMemtable(11, dir)

	err := mem.Insert(kv.KeyValuePair{Key: "c", Value: []byte("3")})
	assert.NoError(t, err)
	err = mem.Insert(kv.KeyValuePair{Key: "a", Value: []byte("1")})
	assert.NoError(t, err)
	err = mem.Insert(kv.KeyValuePair{Key: "b", Value: []byte("2")})
	assert.NoError(t, err)

	imem := NewIMemtable(mem)

	expected := []*kv.KeyValuePair{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
	}

	var actual []*kv.KeyValuePair
	imem.RangeScan(func(pair *kv.KeyValuePair) {
		actual = append(actual, pair)
	})

	assert.Equal(t, expected, actual, "RangeScan should return all kv pairs in sorted order")
}

// TestIMemtable_Id verifies that the ID from the original memtable is preserved.
func TestIMemtableId(t *testing.T) {
	dir := t.TempDir()
	mem := NewMemtable(22, dir)
	imem := NewIMemtable(mem)
	assert.Equal(t, uint64(22), imem.Id())
}

// TestIMemtable_SharedData ensures that the IMemtable shares data with Memtable.
func TestIMemtableSharedData(t *testing.T) {
	dir := t.TempDir()
	mem := NewMemtable(33, dir)
	err := mem.Insert(kv.KeyValuePair{Key: "x", Value: []byte("100")})
	assert.NoError(t, err)

	iMem := NewIMemtable(mem)

	// Insert more into memtable *after* freeze — iMem should see this (shared skiplist)
	err = mem.Insert(kv.KeyValuePair{Key: "y", Value: []byte("200")})
	assert.NoError(t, err)

	val, found := iMem.Search("y")
	assert.True(t, found)
	assert.Equal(t, kv.Value("200"), val)
}
