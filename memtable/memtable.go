// MemTable implementation based on SkipList.
// MemTable is an in-memory, write-optimized data structure used in LSM-tree-based storage systems.
// It temporarily stores incoming key-value writes before they are flushed to disk as sorted files (typically SSTables).
// When data is written, it is first appended to a Write-Ahead Log (WAL) for durability,
// and then inserted into the Memtable. Once the Memtable reaches a certain size threshold,
// it is marked as immutable and handed off to a background process for flushing to disk,
// while a new Memtable is created to handle incoming writes.
// MemTables play a critical role in ensuring high write throughput,
// supporting in-memory reads, and organizing data for efficient flushing and compaction in LSM-based systems.

package memtable

import (
	"fmt"
	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/memtable/skiplist"
	"github.com/xmh1011/go-lsm/util"
	"github.com/xmh1011/go-lsm/wal"
	"path/filepath"
)

const (
	maxMemoryTableSize = 2 * 1024 * 1024 // 2MB
)

// Memtable is an in-memory data structure used to store kv.KeyValuePairs.
// It is implemented on top of a SkipList to maintain ordered data,
// and supports efficient point queries and range scans.
// Each Memtable is also associated with a Write-Ahead Log (WAL),
// which ensures durability and supports recovery in case of crashes.
type Memtable struct {
	id          uint64
	entries     *skiplist.SkipList
	wal         *wal.WAL
	sizeInBytes uint64
}

// NewMemtable creates a new instance of Memtable with WAL.
func NewMemtable(id uint64, walPath string) *Memtable {
	w, err := wal.NewWAL(id, walPath)
	if err != nil {
		log.Errorf("error creating new WAL: %s", err.Error())
		panic(fmt.Errorf("error creating new WAL: %v", err))
	}
	return &Memtable{
		id:      id,
		entries: skiplist.NewSkipList(),
		wal:     w,
	}
}

func NewMemtableWithoutWAL() *Memtable {
	return &Memtable{
		entries: skiplist.NewSkipList(),
		wal:     nil,
	}
}

// Search return true if the key exists in the memtable, otherwise false.
func (t *Memtable) Search(key kv.Key) (kv.Value, bool) {
	return t.entries.Search(key)
}

// Insert inserts a key-value pair into the memtable and WAL.
func (t *Memtable) Insert(pair kv.KeyValuePair) error {
	// WAL: write to log first, then flush to disk.
	if t.wal != nil {
		if err := t.wal.Append(pair); err != nil {
			log.Errorf("error appending %+v to WAL: %s", pair, err.Error())
			return err
		}
	}
	// 估算大小
	t.sizeInBytes += pair.EstimateSize()
	// Writing the key/value pair in the Skiplist.
	t.entries.Add(pair)
	return nil
}

// Delete deletes a key-value pair from the memtable and WAL.
// 无论跳表中是否存在该 key，都应将删除操作记录到 WAL（写入删除标记，即 tombstone），
// 以便在后续合并(compaction)时正确处理删除操作。
// 如果 memtable 中没有这个 key，那么则直接返回 nil
func (t *Memtable) Delete(key kv.Key) error {
	// Delete the record in skiplist first
	if ok := t.entries.Delete(key); ok {
		// if the record is deleted in skiplist, insert a nil value in the wal.
		if t.wal != nil {
			if err := t.wal.Append(kv.KeyValuePair{Key: key, Value: nil}); err != nil {
				log.Errorf("error delete key %s from WAL: %s", key, err.Error())
				return err
			}
		}
	}
	return nil
}

func (t *Memtable) ApproximateSize() uint64 {
	return t.sizeInBytes
}

func (t *Memtable) CanInsert(pair kv.KeyValuePair) bool {
	return t.sizeInBytes+pair.EstimateSize() <= maxMemoryTableSize
}

// RecoverFromWAL constructs up to 10 IMemtable and 1 Memtable from WAL files.
func (t *Memtable) RecoverFromWAL(fileName string) error {
	var err error
	t.id, err = util.ExtractIDFromFileName(fileName)
	if err != nil {
		log.Errorf("invalid WAL file: %s", fileName)
		return fmt.Errorf("invalid WAL file %s: %w", fileName, err)
	}

	pairs := make([]kv.KeyValuePair, 0)
	t.wal, err = wal.Recover(filepath.Join(config.GetWALPath(), fileName), func(pair kv.KeyValuePair) {
		pairs = append(pairs, pair)
	})
	if err != nil {
		log.Errorf("recover WAL %s failed: %v", fileName, err)
		return err
	}

	// Insert all pairs into the memtable
	for _, pair := range pairs {
		err = t.Insert(pair)
		if err != nil {
			log.Errorf("insert pair %+v to memtable failed: %s", pair, err.Error())
			return fmt.Errorf("insert pair %+v to memtable failed: %w", pair, err)
		}
	}

	return nil
}
