package sstable

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/sstable/block"
	"github.com/xmh1011/go-lsm/sstable/bloom"
)

const (
	sstFileSuffix  = "sst"
	levelSuffix    = "level"
	maxSSTableSize = 2 * 1024 * 1024 // 2MB
)

var idGenerator atomic.Uint64

// ResetIDGenerator 重置 SSTable ID 生成器
func ResetIDGenerator() {
	idGenerator.Store(0)
}

// SSTable is an in-memory representation of the file on disk. An SSTable contains the data sorted by key.
// SSTables can be created by flushing an immutable MemTable or by merging SSTables (/compaction).
type SSTable struct {
	id uint64

	level int

	filePath string

	// Header 记录 SSTable 的元数据信息，包括最小 Key、最大 Key 等
	Header *block.Header

	// FilterBlock 合并了结构图中的 Filter 和 MetaIndexBlock
	// 记录 Filter 的相关信息
	FilterBlock *bloom.Filter

	// IndexBlock 是数据块的索引部分，记录每个 Data Blcok 的起始位置、大小和最大 Key 值
	IndexBlock *block.IndexBlock // 按 key 顺序排列的索引项

	// DataBlock 是 SSTable 的数据块部分，包含实际的value数据。
	DataBlock *block.DataBlock

	// Footer 是 SSTable 的尾部信息，包含了 IndexBlock 的位置等元数据
	Footer *block.Footer
}

func NewSSTable() *SSTable {
	return &SSTable{
		id:          idGenerator.Add(1),
		IndexBlock:  block.NewIndexBlock(),
		FilterBlock: bloom.DefaultBloomFilter(),
		Footer:      block.NewFooter(),
		Header:      block.NewHeader("", ""),
		DataBlock:   block.NewDataBlock(),
	}
}

func NewRecoverSSTable(level int) *SSTable {
	return &SSTable{
		level:       level,
		IndexBlock:  block.NewIndexBlock(),
		FilterBlock: bloom.DefaultBloomFilter(),
		Footer:      block.NewFooter(),
		Header:      block.NewHeader("", ""),
		DataBlock:   block.NewDataBlock(),
	}
}

func NewSSTableWithLevel(level int) *SSTable {
	table := NewSSTable()
	table.level = level
	table.filePath = sstableFilePath(table.id, level, config.GetSSTablePath())
	return table
}

// DecodeFrom 从给定文件路径加载 SSTable 到内存中。不加载 DataBlock 的内容。
func (t *SSTable) DecodeFrom(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		log.Errorf("open file %s error: %s", filePath, err.Error())
		return fmt.Errorf("open file error: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("close file %s error: %s", filePath, err.Error())
		}
	}(file)
	t.filePath = filePath

	if err = t.Header.DecodeFrom(file); err != nil {
		log.Errorf("decode Header from file %s error: %s", filePath, err.Error())
		return fmt.Errorf("decode Header failed: %w", err)
	}

	if err = t.FilterBlock.DecodeFrom(file); err != nil {
		log.Errorf("decode FilterBlock from file %s error: %s", filePath, err.Error())
		return fmt.Errorf("decode FilterBlock failed: %w", err)
	}

	// 定位到文件末尾，读取 Footer
	if err = t.DecodeFooterFrom(file); err != nil {
		log.Errorf("decode Footer from file %s error: %s", filePath, err.Error())
		return fmt.Errorf("decode Footer failed: %w", err)
	}

	// 根据 Footer 定位 IndexBlock
	if _, err = file.Seek(t.Footer.IndexHandle.Offset, io.SeekStart); err != nil {
		log.Errorf("seek to index block position in file %s error: %s", filePath, err.Error())
		return fmt.Errorf("seek to index block position failed: %w", err)
	}
	if err = t.IndexBlock.DecodeFrom(file, t.Footer.IndexHandle.Size); err != nil {
		log.Errorf("decode IndexBlock from file %s error: %s", filePath, err.Error())
		return fmt.Errorf("decode IndexBlock failed: %w", err)
	}

	return nil
}

// EncodeTo 将 SSTable 的各个部分（DataBlocks、IndexBlock、FilterBlock、Footer）依次写入文件中
func (t *SSTable) EncodeTo(filePath string) error {
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		log.Errorf("create directory %s error: %s", filepath.Dir(filePath), err.Error())
		return fmt.Errorf("create directory failed: %w", err)
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("open file %s error: %s", filePath, err.Error())
		return fmt.Errorf("open file error: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("close file %s error: %s", filePath, err.Error())
		}
	}(file)
	t.filePath = filePath

	if err = t.Header.EncodeTo(file); err != nil {
		log.Errorf("encode Header to file %s error: %s", filePath, err.Error())
		return fmt.Errorf("encode Header failed: %w", err)
	}

	if err = t.FilterBlock.EncodeTo(file); err != nil {
		log.Errorf("encode FilterBlock to file %s error: %s", filePath, err.Error())
		return fmt.Errorf("encode FilterBlock failed: %w", err)
	}

	// 记录 DataBlock 的起始偏移量和大小
	if t.Footer.DataHandle.Offset, err = file.Seek(0, io.SeekCurrent); err != nil {
		log.Errorf("seek to index block position error: %s", err.Error())
		return fmt.Errorf("seek to index block position failed: %w", err)
	}
	for i, value := range t.DataBlock.Entries {
		if t.IndexBlock.Indexes[i].Offset, err = file.Seek(0, io.SeekCurrent); err != nil {
			log.Errorf("seek to current position error: %s", err.Error())
			return fmt.Errorf("seek to current position failed: %w", err)
		}
		size, err := value.EncodeTo(file)
		if err != nil {
			log.Errorf("encode DataBlock to file %s error: %s", filePath, err.Error())
			return fmt.Errorf("encode DataBlock failed: %w", err)
		}
		t.Footer.DataHandle.Size += size
	}

	// 记录 IndexBlock 的起始偏移量和大小
	if t.Footer.IndexHandle.Offset, err = file.Seek(0, io.SeekCurrent); err != nil {
		log.Errorf("seek to index block position error: %s", err.Error())
		return fmt.Errorf("seek to index block position failed: %w", err)
	}
	if t.Footer.IndexHandle.Size, err = t.IndexBlock.Encode(file); err != nil {
		log.Errorf("encode IndexBlock to file %s error: %s", filePath, err.Error())
		return fmt.Errorf("encode IndexBlock failed: %w", err)
	}

	if err = t.Footer.EncodeTo(file); err != nil {
		log.Errorf("encode Footer to file %s error: %s", filePath, err.Error())
		return fmt.Errorf("encode Footer failed: %w", err)
	}

	return nil
}

func (t *SSTable) DecodeFooterFrom(file *os.File) error {
	// 定位到文件末尾，读取 Footer
	fileInfo, err := file.Stat()
	if err != nil {
		log.Errorf("get file info for %s error: %s", t.filePath, err.Error())
		return fmt.Errorf("get file info failed: %w", err)
	}
	if _, err = file.Seek(fileInfo.Size()-block.FooterSize, io.SeekStart); err != nil {
		log.Errorf("seek to footer position in file %s error: %s", t.filePath, err.Error())
		return fmt.Errorf("seek to footer position failed: %w", err)
	}
	if err = t.Footer.DecodeFrom(file); err != nil {
		log.Errorf("decode Footer from file %s error: %s", t.filePath, err.Error())
		return fmt.Errorf("decode Footer failed: %w", err)
	}

	return nil
}

func (t *SSTable) DecodeDataBlock(file *os.File) error {
	if _, err := file.Seek(t.Footer.DataHandle.Offset, io.SeekStart); err != nil {
		log.Errorf("seek to IndexBlock position error: %s", err.Error())
		return fmt.Errorf("seek to IndexBlock position failed: %w", err)
	}
	if err := t.DataBlock.DecodeFrom(file, t.Footer.DataHandle.Size); err != nil {
		log.Errorf("decode DataBlock from file error: %s", err.Error())
		return fmt.Errorf("decode DataBlock failed: %w", err)
	}

	return nil
}

func (t *SSTable) GetDataBlockFromFile(path string) ([]kv.KeyValuePair, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Errorf("open file %s error: %s", path, err.Error())
		return nil, fmt.Errorf("open file error: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("close file %s error: %s", path, err.Error())
		}
	}(file)

	if err = t.DecodeDataBlock(file); err != nil {
		log.Errorf("decode DataBlock from file %s error: %s", path, err.Error())
		return nil, fmt.Errorf("decode DataBlock failed: %w", err)
	}

	return t.GetKeyValuePairs()
}

func (t *SSTable) GetKeyValuePairs() ([]kv.KeyValuePair, error) {
	// 校验 DataBlock 和 IndexBlock 是否匹配
	if t.DataBlock.Len() == 0 || t.IndexBlock.Len() == 0 {
		log.Warnf("SSTable %s has no data or index entries", t.filePath)
		return nil, nil
	}
	if t.DataBlock.Len() != t.IndexBlock.Len() {
		log.Errorf("SSTable %s has mismatched DataBlock and IndexBlock entries", t.filePath)
		return nil, errors.New("mismatched DataBlock and IndexBlock entries")
	}

	pairs := make([]kv.KeyValuePair, 0)
	for i, entry := range t.DataBlock.Entries {
		pairs = append(pairs, kv.KeyValuePair{
			Key:   t.IndexBlock.Indexes[i].Key,
			Value: entry,
		})
	}

	return pairs, nil
}

// GetValueByOffset 根据给定的偏移量从 SSTable 中获取对应的 Value。
func (t *SSTable) GetValueByOffset(offset int64) (kv.Value, error) {
	file, err := os.Open(t.filePath)
	if err != nil {
		log.Errorf("open file %s error: %s", t.filePath, err.Error())
		return nil, fmt.Errorf("open file error: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("close file %s error: %s", t.filePath, err.Error())
		}
	}(file)

	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		log.Errorf("seek to offset error: %s", err.Error())
		return nil, fmt.Errorf("seek to offset failed: %w", err)
	}

	value := kv.Value{}
	if err = value.DecodeFrom(file); err != nil {
		log.Errorf("decode value error: %s", err.Error())
		return nil, fmt.Errorf("decode value failed: %w", err)
	}

	return value, nil
}

// MayContain uses bloom filter to determine if the given key maybe present in the SSTable.
// Returns true if the key MAYBE present, false otherwise.
func (t *SSTable) MayContain(key kv.Key) bool {
	if t.Header.MinKey > key || t.Header.MaxKey < key {
		return false
	}
	return t.FilterBlock.MayContain(key)
}

// ID returns the id of SSTable.
func (t *SSTable) ID() uint64 {
	return t.id
}

// Remove 释放 SSTable
func (t *SSTable) Remove() error {
	if err := os.Remove(t.filePath); err != nil {
		log.Errorf("remove file %s error: %s", t.filePath, err.Error())
		return err
	}
	return nil
}

// Add 加入新的 KV 对到 SSTable
func (t *SSTable) Add(pair *kv.KeyValuePair) {
	t.DataBlock.Add(pair.Value)
	t.IndexBlock.Add(pair.Key, 0)
	t.FilterBlock.Add([]byte(pair.Key))
}

func (t *SSTable) FilePath() string {
	return t.filePath
}

// sstableFilePath returns the SSTable filepath which consists of rootPath/id.sst.
func sstableFilePath(id uint64, level int, rootPath string) string {
	return filepath.Join(sstableLevelPath(level, rootPath), fmt.Sprintf("%v.%s", id, sstFileSuffix))
}

func sstableLevelPath(level int, rootPath string) string {
	return filepath.Join(rootPath, fmt.Sprintf("%d-%s", level, levelSuffix))
}
