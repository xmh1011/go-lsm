// Immutable MemTable implementation
// IMemTable is a read-only memtable, used for flush/compaction.
// The IMemTable itself is an in-memory, read-only data structure designed to freeze an existing MemTable
// so that its data can be asynchronously flushed to disk in the background (e.g., into an SSTable or other persistent format).
// Therefore, the IMemTable does not need to implement the flush (persist-to-disk) operation directly.
// It simply acts as a read-only snapshot, providing access through query and scan interfaces.
// In other words, the flush process is typically handled by a separate background thread or scheduler,
// which reads data from the IMemTable and writes it to disk in a sorted format.
// As such, the IMemTable itself is not responsible for managing persistence logic.

package memtable

import (
	"os"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/memtable/skiplist"
	"github.com/xmh1011/go-lsm/wal"
)

// IMemTable is an immutable memtable, used for flush/compaction.
// It is read-only and supports only Search and Scan operations.
type IMemTable struct {
	id      uint64
	entries *skiplist.SkipList
	wal     *wal.WAL
}

// NewIMemTable creates an IMemTable from an existing MemTable.
// Used when memtable is frozen for flushing.
func NewIMemTable(mem *MemTable) *IMemTable {
	return &IMemTable{
		id:      mem.id,
		entries: mem.entries,
		wal:     mem.wal,
	}
}

// Search searches for a key in the immutable memtable.
func (t *IMemTable) Search(key kv.Key) (kv.Value, bool) {
	return t.entries.Search(key)
}

// RangeScan scans all key-value pairs in order and calls the callback.
func (t *IMemTable) RangeScan(callback func(*kv.KeyValuePair)) {
	iter := skiplist.NewSkipListIterator(t.entries)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		callback(iter.Pair())
	}
}

// ID returns the ID of this IMemTable.
func (t *IMemTable) ID() uint64 {
	return t.id
}

func (t *IMemTable) Clean() {
	err := t.wal.DeleteFile()
	if err != nil && !os.IsNotExist(err) {
		log.Errorf("failed to clean WAL file %d: %s", t.id, err.Error())
	}
}
