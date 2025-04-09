package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/sstable/block"
)

const (
	sstFileSuffix  = "sst"
	levelSuffix    = "level"
	maxSSTableSize = 2 * 1024 * 1024 // 2MB
)

var idGenerator atomic.Uint64

// SSTable is an in-memory representation of the file on disk. An SSTable contains the data sorted by key.
// SSTables can be created by flushing an immutable Memtable or by merging SSTables (/compaction).
type SSTable struct {
	id uint64

	file *os.File

	level int

	filePath string

	// DataBlocks 由多个 DataRecord 组成，用于存储 key/value 记录。
	// 在 SSTable 中，一个 DataBlocks 是一段连续写入的不可变数据区。
	DataBlocks []*block.DataBlock

	// IndexBlock 是数据块的索引部分，记录每个 Data Blcok 的起始位置、大小和最大 Key 值
	IndexBlock *block.IndexBlock // 按 key 顺序排列的索引项

	// FilterBlock 合并了结构图中的 Filter 和 MetaIndexBlock
	// 记录 Filter 的相关信息
	FilterBlock *block.FilterBlock

	// Footer 指向索引的索引，固定长度
	Footer block.Footer

	// references 是 SSTable 的引用计数，用于管理 SSTable 的生命周期
	// 通常我们在 compaction 之后 会尝试移除旧的 SSTable。
	// 此时，需要检查它是否仍然被其他地方使用。如果还有引用（ref > 0），就不能删除。
	// TODO: 未实现
	references atomic.Int64
}

func NewSSTable() *SSTable {
	return &SSTable{
		id:          idGenerator.Add(1),
		DataBlocks:  make([]*block.DataBlock, 0),
		IndexBlock:  block.NewIndexBlock(),
		FilterBlock: block.NewFilterBlock(0, 0),
	}
}

func NewRecoverSSTable(level int) *SSTable {
	return &SSTable{
		level:       level,
		DataBlocks:  make([]*block.DataBlock, 0),
		IndexBlock:  block.NewIndexBlock(),
		FilterBlock: block.NewFilterBlock(0, 0),
	}
}

func NewSSTableWithLevel(level int) *SSTable {
	table := NewSSTable()
	table.level = level
	table.filePath = sstableFilePath(table.id, level, config.GetSSTablePath())
	return table
}

func (t *SSTable) DecodeMetaData(filePath string) error {
	var err error
	t.file, err = os.Open(filePath)
	if err != nil {
		log.Errorf("open file %s error: %s", filePath, err.Error())
		return fmt.Errorf("open file error: %w", err)
	}
	t.filePath = filePath

	// 1. 读取 Footer（固定 footerSize 字节）
	err = t.Footer.DecodeFrom(t.file)
	if err != nil {
		log.Errorf("decode footer error: %s", err.Error())
		return fmt.Errorf("decode footer error: %w", err)
	}

	// 2. 根据 Footer 读取 IndexBlock
	err = t.IndexBlock.DecodeFrom(t.file, t.Footer.IndexHandle)
	if err != nil {
		log.Errorf("decode index block error: %s", err.Error())
		return fmt.Errorf("decode index block error: %w", err)
	}

	// 3. 根据 Footer 读取 FilterBlock
	err = t.FilterBlock.DecodeFrom(t.file, t.Footer.MetaIndexHandle)
	if err != nil {
		log.Errorf("decode meta index block error: %s", err.Error())
		return fmt.Errorf("decode meta index block error: %w", err)
	}

	return nil
}

func (t *SSTable) DecodeDataBlocks() error {
	var err error
	// 读取 DataBlocks
	t.DataBlocks = make([]*block.DataBlock, len(t.IndexBlock.Indexes))
	for i, index := range t.IndexBlock.Indexes {
		t.DataBlocks[i] = block.NewDataBlock()
		err = t.DataBlocks[i].DecodeFrom(t.file, index.Handle)
		if err != nil {
			log.Errorf("decode data block error: %s", err.Error())
			return fmt.Errorf("decode data block error: %w", err)
		}
	}

	return nil
}

// DecodeFrom 从给定文件路径加载 SSTable 到内存中。
func (t *SSTable) DecodeFrom(filePath string) error {
	var err error
	t.file, err = os.Open(filePath)
	if err != nil {
		log.Errorf("open file %s error: %s", filePath, err.Error())
		return fmt.Errorf("open file error: %w", err)
	}
	defer t.Close()

	if err = t.DecodeMetaData(filePath); err != nil {
		log.Errorf("decode metadata error: %s", err.Error())
		return err
	}

	// 读取 DataBlocks
	if err = t.DecodeDataBlocks(); err != nil {
		log.Errorf("decode data blocks error: %s", err.Error())
		return err
	}

	return nil
}

// EncodeTo 将 SSTable 的各个部分（DataBlocks、IndexBlock、FilterBlock、Footer）依次写入文件中
func (t *SSTable) EncodeTo(filePath string) error {
	var err error
	if err = os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		log.Errorf("create directory %s error: %s", filepath.Dir(filePath), err.Error())
		return fmt.Errorf("create directory failed: %w", err)
	}
	t.file, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("open file %s error: %s", filePath, err.Error())
		return fmt.Errorf("open file error: %w", err)
	}
	t.filePath = filePath

	// 清空 IndexBlock 信息
	t.IndexBlock = block.NewIndexBlock()
	t.IndexBlock.Indexes = make([]*block.IndexEntry, len(t.DataBlocks))

	// 1. DataBlocks：依次写入各个数据块，并记录它们的 Handle 到 IndexBlock.Indexes
	for i, dataBlock := range t.DataBlocks {
		// 编码写入当前 DataBlock
		t.IndexBlock.Indexes[i] = block.NewIndexEntry()
		t.IndexBlock.Indexes[i].Handle, err = dataBlock.EncodeTo(t.file)
		if err != nil {
			log.Errorf("encode DataBlock failed: %s", err.Error())
			return fmt.Errorf("encode DataBlock failed: %w", err)
		}
		// 计算当前 DataBlock 的分割键
		if len(dataBlock.Records) > 0 {
			if i == 0 {
				t.IndexBlock.StartKey = dataBlock.Records[0].Key
			}
			t.IndexBlock.Indexes[i].SeparatorKey = dataBlock.Records[len(dataBlock.Records)-1].Key
		}
	}

	// 2. IndexBlock：写入数据块的索引信息，并记录其 Handle。
	t.Footer.IndexHandle, err = t.IndexBlock.EncodeTo(t.file)
	if err != nil {
		log.Errorf("encode IndexBlock failed: %s", err.Error())
		return fmt.Errorf("encode IndexBlock failed: %w", err)
	}

	// 3. FilterBlock：写入过滤器数据，并记录其 Handle。
	t.Footer.MetaIndexHandle, err = t.FilterBlock.EncodeTo(t.file)
	if err != nil {
		log.Errorf("encode FilterBlock failed: %s", err.Error())
		return fmt.Errorf("encode FilterBlock failed: %w", err)
	}

	// 4. Footer：写入 Footer，同时将 IndexBlock 和 FilterBlock 的 Handle 写入 Footer 中。
	err = t.Footer.EncodeTo(t.file)
	if err != nil {
		log.Errorf("encode Footer failed: %s", err.Error())
		return fmt.Errorf("encode Footer failed: %w", err)
	}

	return nil
}

// incrementReference increments the references of the SSTable.
// A reference is typically used when an SSTable is to be removed (usually after compaction).
// An SSTable with a reference (/usage) > 0 can not be removed unless all the references to the SSTable are dropped.
func (t *SSTable) incrementReference() {
	t.references.Add(1)
}

// LoadSpecifiedDataBlock 根据块号加载 DataBlocks 中对应的记录，并返回 Block 对象。
func (t *SSTable) LoadSpecifiedDataBlock(blockIdx int) *block.DataBlock {
	if blockIdx < 0 || blockIdx >= len(t.IndexBlock.Indexes) {
		log.Errorf("block index out of range: %d", blockIdx)
		return nil
	}

	dataBlock := block.NewDataBlock()
	err := dataBlock.DecodeFrom(t.file, t.IndexBlock.Indexes[blockIdx].Handle)
	if err != nil {
		log.Errorf("decode data block error: %s", err.Error())
		return nil
	}

	return dataBlock
}

// MayContain uses bloom filter to determine if the given key maybe present in the SSTable.
// Returns true if the key MAYBE present, false otherwise.
func (t *SSTable) MayContain(key kv.Key) bool {
	return t.FilterBlock.MayContain(key)
}

// Id returns the id of SSTable.
func (t *SSTable) Id() uint64 {
	return t.id
}

// TotalReferences returns the total references to the SSTable.
func (t *SSTable) TotalReferences() int64 {
	return t.references.Load()
}

// Remove 释放 SSTable
func (t *SSTable) Remove() error {
	if err := t.file.Close(); err != nil {
		log.Errorf("close file %s error: %s", t.file.Name(), err.Error())
		return err
	}
	if err := os.Remove(t.file.Name()); err != nil {
		log.Errorf("remove file %s error: %s", t.file.Name(), err.Error())
		return err
	}
	return nil
}

func (t *SSTable) Close() {
	if t.file != nil {
		err := t.file.Close()
		if err != nil {
			log.Errorf("close file %s error: %s", t.file.Name(), err.Error())
			return
		}
	}
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
