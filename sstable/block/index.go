/*
IndexBlock structure:
记录每个 Data Block 的起始位置和最大的key
由多个 IndexEntry 构成，每个 IndexEntry 的结构如下：

┌────────────────┬───────────────────────────────────┐
│ separator key  │          handle (2)               │
├────────────────┴───────────────────────────────────┤
│ [len+data]     │ offset + size (uvarint + uvarint) │
└────────────────┴───────────────────────────────────┘

说明：
- startKey 和 endKey 均为 [keyLen][keyData] 编码格式
- handle 表示对应 DataBlocks 的 offset 和 size
*/

package block

import (
	"fmt"
	"io"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
)

// IndexEntry 表示一个数据块的 key 范围及对应文件位置
// 记录 SeparatorKey，方便 compaction 时快速定位数据块，进行归并排序
// SeparatorKey 为第一个数据块的最小key和最大key，其余数据块的最大key
type IndexEntry struct {
	SeparatorKey kv.Key
	Handle       Handle
}

// IndexBlock 表示 SSTable 的索引块，按 Key 顺序记录每个 DataBlock 的范围和位置信息
type IndexBlock struct {
	StartKey kv.Key
	Indexes  []*IndexEntry // 按 SeparatorKey 升序排列的索引项
}

func NewIndexBlock() *IndexBlock {
	return &IndexBlock{
		Indexes: make([]*IndexEntry, 0),
	}
}

func NewIndexEntry() *IndexEntry {
	return &IndexEntry{
		Handle: Handle{},
	}
}

// DecodeFrom 从给定文件句柄中读取 IndexBlock
func (i *IndexBlock) DecodeFrom(r io.ReadSeeker, handle Handle) error {
	if _, err := r.Seek(int64(handle.Offset), io.SeekStart); err != nil {
		log.Errorf("seek to index block failed: %s", err.Error())
		return fmt.Errorf("seek to index block failed: %w", err)
	}
	// 首先读取 startKey 并判断读取的字节数
	if err := i.StartKey.DecodeFrom(r); err != nil {
		log.Errorf("decode start key failed: %s", err.Error())
		return fmt.Errorf("decode start key failed: %w", err)
	}
	endPos, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("get current offset failed: %s", err.Error())
		return fmt.Errorf("get current offset failed: %w", err)
	}
	keySize := endPos - int64(handle.Offset)
	indexSize := handle.Size - uint64(keySize)

	var bytesRead uint64
	for bytesRead < indexSize {
		startPos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Errorf("get current offset failed: %s", err.Error())
			return fmt.Errorf("get current offset failed: %w", err)
		}

		entry := NewIndexEntry()
		if err := entry.SeparatorKey.DecodeFrom(r); err != nil {
			log.Errorf("decode end key failed: %s", err.Error())
			return fmt.Errorf("decode end key failed: %w", err)
		}
		if err := entry.Handle.DecodeFrom(r); err != nil {
			log.Errorf("decode handle failed: %s", err.Error())
			return fmt.Errorf("decode handle failed: %w", err)
		}
		i.Indexes = append(i.Indexes, entry)

		endPos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Errorf("get end offset failed: %s", err.Error())
			return fmt.Errorf("get end offset failed: %w", err)
		}
		bytesRead += uint64(endPos - startPos)
	}
	if bytesRead != indexSize {
		log.Errorf("index block bytes read (%d) != expected (%d)", bytesRead, handle.Size)
		return fmt.Errorf("index block bytes read (%d) != expected (%d)", bytesRead, handle.Size)
	}

	return nil
}

// EncodeTo 将 IndexBlock 编码写入 writer 并返回 Handle 信息
func (i *IndexBlock) EncodeTo(w io.WriteSeeker) (Handle, error) {
	var handle Handle
	startOffset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("get current offset failed: %s", err.Error())
		return handle, fmt.Errorf("get current offset failed: %w", err)
	}
	handle.Offset = uint64(startOffset)
	// 写入 StartKey
	if err := i.StartKey.EncodeTo(w); err != nil {
		log.Errorf("encode start key failed: %s", err.Error())
		return handle, fmt.Errorf("encode start key failed: %w", err)
	}

	for _, entry := range i.Indexes {
		if err := entry.SeparatorKey.EncodeTo(w); err != nil {
			log.Errorf("encode end key failed: %s", err.Error())
			return handle, fmt.Errorf("encode end key failed: %w", err)
		}
		if err := entry.Handle.EncodeTo(w); err != nil {
			log.Errorf("encode handle failed: %s", err.Error())
			return handle, fmt.Errorf("encode handle failed: %w", err)
		}
	}

	endOffset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("get end offset failed: %s", err.Error())
		return handle, fmt.Errorf("get end offset failed: %w", err)
	}
	handle.Size = uint64(endOffset - startOffset)

	return handle, nil
}
