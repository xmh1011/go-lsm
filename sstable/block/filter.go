/*
┌─────────────────────────────────────────────┐
│             Filter Block Layout             │
├──────────────┬──────────────────────────────┤
│ MetaIndex    │   Bloom Filter Data          │
└──────────────┴──────────────────────────────┘
*/

package block

import (
	"fmt"
	"io"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/sstable/bloom"
)

// FilterBlock 用于记录 filter block 的相关信息。
// SSTable 中 FilterBlock 只有一条记录，key 为 "filter."+filter_policy 的名字，
// value 为该 filter block 的 Handle（包含起始位置和大小）。
type FilterBlock struct {
	// Key 格式为 "filter.<filter_policy_name>"
	Key       kv.Key        // 在本次实现中，统一用 bloom 过滤器，因此在 SSTable 中不记录该 key
	MetaIndex Handle        // 对应 filter block 中 Bloom Filter 数据的偏移量和大小
	Filter    *bloom.Filter // 实际布隆过滤器指针
}

func NewFilterBlock(m, k uint) *FilterBlock {
	return &FilterBlock{
		Filter: bloom.DefaultBloomFilter(),
	}
}

// DecodeFrom 从给定文件中根据 Handle 解码 FilterBlock
func (f *FilterBlock) DecodeFrom(r io.ReadSeeker, handle Handle) error {
	// 1. 跳转到 FilterBlock 的起始偏移位置
	if _, err := r.Seek(int64(handle.Offset), io.SeekStart); err != nil {
		log.Errorf("seek to metaindex block offset failed: %s", err.Error())
		return fmt.Errorf("seek to metaindex block offset failed: %w", err)
	}

	// 2. 读取 MetaIndex（即 Bloom Filter 的 Handle）
	if err := f.MetaIndex.DecodeFrom(r); err != nil {
		log.Errorf("decode metaindex block handle failed: %s", err.Error())
		return fmt.Errorf("decode metaindex block handle failed: %w", err)
	}

	// 3. 跳转到 Bloom Filter 的位置
	if _, err := r.Seek(int64(f.MetaIndex.Offset), io.SeekStart); err != nil {
		log.Errorf("seek to filter block offset failed: %s", err.Error())
		return fmt.Errorf("seek to filter block offset failed: %w", err)
	}

	f.Filter = bloom.NewBloomFilter(1, 1) // 初始化为非 nil 指针（长度随意，会被覆盖）
	if err := f.Filter.DecodeFrom(r, f.MetaIndex.Size); err != nil {
		log.Errorf("decode filter block failed: %s", err.Error())
		return fmt.Errorf("decode filter block failed: %w", err)
	}

	return nil
}

func (f *FilterBlock) EncodeTo(w io.WriteSeeker) (Handle, error) {
	var handle Handle
	// 记录当前起始写入偏移量
	startOffset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("get current offset failed: %s", err.Error())
		return handle, fmt.Errorf("get current offset failed: %w", err)
	}
	handle.Offset = uint64(startOffset)

	// 预留固定 16 字节空间写入 MetaIndex 占位符
	placeholder := make([]byte, 16)
	n, err := w.Write(placeholder)
	if err != nil || n != 16 {
		log.Errorf("write placeholder handle failed")
		return handle, fmt.Errorf("write placeholder handle failed: %w", err)
	}

	// 记录 Bloom Filter 数据写入的起始偏移量
	bloomOffset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("get bloom filter offset failed: %s", err.Error())
		return handle, fmt.Errorf("get bloom filter offset failed: %w", err)
	}

	// 写入 Bloom Filter 数据，并记录写入的字节数
	bloomSize, err := f.Filter.EncodeTo(w)
	if err != nil {
		log.Errorf("encode bloom filter failed: %s", err.Error())
		return handle, fmt.Errorf("encode bloom filter failed: %w", err)
	}

	// 设置 MetaIndex 为 Bloom Filter 数据的偏移量和大小
	f.MetaIndex = Handle{
		Offset: uint64(bloomOffset),
		Size:   bloomSize,
	}

	// 跳转回起始位置，覆盖写入正确的 MetaIndex（必须保证写入字节数为 16 字节）
	if _, err := w.Seek(startOffset, io.SeekStart); err != nil {
		log.Errorf("seek to start offset failed: %s", err.Error())
		return handle, fmt.Errorf("seek to start offset failed: %w", err)
	}
	if err := f.MetaIndex.EncodeTo(w); err != nil {
		log.Errorf("rewrite metaindex handle failed: %s", err.Error())
		return handle, fmt.Errorf("rewrite metaindex handle failed: %w", err)
	}

	// 回到文件末尾
	if _, err := w.Seek(0, io.SeekEnd); err != nil {
		log.Errorf("seek to end failed: %s", err.Error())
		return handle, fmt.Errorf("seek to end failed: %w", err)
	}

	// 计算从起始位置到当前偏移量的总写入字节数
	endOffset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("get end offset failed: %s", err.Error())
		return handle, fmt.Errorf("get end offset failed: %w", err)
	}
	handle.Size = uint64(endOffset - startOffset)

	return handle, nil
}

// MayContain 查询 key 是否可能存在于布隆过滤器中
func (f *FilterBlock) MayContain(key kv.Key) bool {
	return f.Filter.MayContain(key)
}

func (f *FilterBlock) Add(key kv.Key) {
	f.Filter.Add([]byte(key))
}
