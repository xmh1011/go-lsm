// WAL design for each memtable
// The Write-Ahead Log (WAL) is a fundamental mechanism used to ensure durability and crash recovery
// in storage systems like databases and LSM-tree-based key-value stores.
// When a write operation (such as inserting or deleting a key-value pair) occurs,
// the data is first appended to the WAL file on disk before being applied to the in-memory structure (e.g., MemTable).
// This append-only log ensures that, in the event of a crash or system failure,
// the system can replay the WAL to restore the in-memory state to its last consistent point.
// Since the WAL is written sequentially, it offers high write throughput and minimal I/O overhead.
// Once the data in memory (MemTable) is flushed to disk in a more structured format (like an SSTable),
// the corresponding WAL file can be safely deleted. In LSM-based systems,
// WAL plays a crucial role in achieving durability, fault tolerance, and write efficiency.

package wal

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
)

const (
	defaultWALFileMode   = 0666
	defaultWALFileSuffix = "wal"
)

// WAL implementation
// 在项目设计中，每个memtable拥有独立WAL且单线程写入，因此不需要考虑加锁的问题
// 如果WAL是单实例、全局持久化日志文件（common to all MemTables），多个协程可能同时写入或重放数据，必须加锁。
type WAL struct {
	file *os.File
	path string
}

func init() {
	// Create the WAL directory if it doesn't exist
	if err := os.MkdirAll(config.GetWALPath(), os.ModePerm); err != nil {
		log.Errorf("failed to create WAL directory: %s", err.Error())
	}
}

// NewWAL creates a new instance of WAL for the specified memtable id and a directory path.
// This implementation has WAL for each memtable.
// Every write to memtable involves writing every key/value pair from the batch to WAL.
// This implementation writes every key/value pair from the batch to WAL individually.
func NewWAL(id uint64, path string) (*WAL, error) {
	wal := &WAL{
		path: CreateWalPath(id, path),
	}
	var err error
	wal.file, err = os.OpenFile(wal.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, defaultWALFileMode)
	if err != nil {
		log.Errorf("WAL: failed to open WAL file: %s", err.Error())
		return nil, err
	}
	return wal, nil
}

// CreateWalPath creates a WAL path for the memtable with id.
func CreateWalPath(id uint64, walDirectoryPath string) string {
	return filepath.Join(walDirectoryPath, fmt.Sprintf("%v.%s", id, defaultWALFileSuffix))
}

// Sync flushes the file to disk.
func (w *WAL) Sync() error {
	return w.file.Sync()
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	return w.file.Close()
}

// DeleteFile deletes the WAL file.
func (w *WAL) DeleteFile() error {
	return os.Remove(w.path)
}

// Append writes a KeyValuePair in JSON format to the WAL file.
func (w *WAL) Append(pair kv.KeyValuePair) error {
	if err := pair.EncodeTo(w.file); err != nil {
		log.Errorf("failed to write wal, key: %s, error: %s", pair.Key, err.Error())
		return fmt.Errorf("failed to write wal, key: %s: %w", pair.Key, err)
	}

	return nil
}

// Recover reads the WAL file and calls the callback function for each KeyValuePair.
func Recover(path string, callback func(pair kv.KeyValuePair)) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("open wal file failed: %s", err.Error())
		return nil, fmt.Errorf("open wal file failed: %w", err)
	}
	raw, err := io.ReadAll(file)
	if err != nil {
		log.Errorf("read wal file failed: %s", err.Error())
		return nil, fmt.Errorf("read wal file failed: %w", err)
	}

	buf := bytes.NewReader(raw)
	for buf.Len() > 0 {
		var pair kv.KeyValuePair
		err := pair.DecodeFrom(buf)
		if err != nil {
			log.Errorf("failed to read wal %s, error: %s", file.Name(), err.Error())
			return nil, fmt.Errorf("failed to read wal %s: %w", file.Name(), err)
		}

		// 回调处理有效数据
		callback(pair)
	}

	return &WAL{file: file, path: path}, nil
}
