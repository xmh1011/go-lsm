package block

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/xmh1011/go-lsm/log"
)

// Footer 表示 SSTable 的文件尾，固定长度（16 字节）。
type Footer struct {
	DataHandle  Handle // 数据块的 Handle
	IndexHandle Handle // 索引块的 Handle
}

type Handle struct {
	Offset int64 // 数据块的起始偏移量
	Size   int64 // 数据块的大小
}

const (
	FooterSize = 32 // 16 (Data handle) + 16 (Index handle) 字节
	HandleSize = 16 // 每个 handle 的大小（8 字节偏移 + 8 字节大小）
)

// NewFooter 创建一个新的 Footer 实例
func NewFooter() *Footer {
	return &Footer{
		DataHandle:  NewHandle(0, 0),
		IndexHandle: NewHandle(0, 0),
	}
}

func NewHandle(offset, size int64) Handle {
	return Handle{
		Offset: offset,
		Size:   size,
	}
}

// EncodeTo 将 Footer 编码到 io.Writer 中
func (f *Footer) EncodeTo(w io.Writer) error {
	if err := f.DataHandle.EncodeTo(w); err != nil {
		log.Errorf("encode data handle failed: %s", err.Error())
		return fmt.Errorf("encode data handle failed: %w", err)
	}

	if err := f.IndexHandle.EncodeTo(w); err != nil {
		log.Errorf("encode index handle failed: %s", err.Error())
		return fmt.Errorf("encode index handle failed: %w", err)
	}

	return nil
}

// DecodeFrom 从 io.Reader 中解码 Footer
func (f *Footer) DecodeFrom(r io.Reader) error {
	if err := f.DataHandle.DecodeFrom(r); err != nil {
		log.Errorf("decode data handle failed: %s", err.Error())
		return fmt.Errorf("decode data handle failed: %w", err)
	}

	if err := f.IndexHandle.DecodeFrom(r); err != nil {
		log.Errorf("decode index handle failed: %s", err.Error())
		return fmt.Errorf("decode index handle failed: %w", err)
	}

	return nil
}

// DecodeFrom 从文件读取 Handle
func (h *Handle) DecodeFrom(r io.Reader) error {
	buf := make([]byte, HandleSize)

	if _, err := io.ReadFull(r, buf); err != nil {
		log.Errorf("decode footer failed: %s", err.Error())
		return fmt.Errorf("decode footer failed: %w", err)
	}

	h.Offset = int64(binary.LittleEndian.Uint64(buf[0:8]))
	h.Size = int64(binary.LittleEndian.Uint64(buf[8:16]))

	return nil
}

// EncodeTo 将 Handle 写入文件
func (h *Handle) EncodeTo(w io.Writer) error {
	buf := make([]byte, HandleSize)

	// 写入 Offset 和 Size
	binary.LittleEndian.PutUint64(buf[0:8], uint64(h.Offset))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(h.Size))

	// 写入文件
	if _, err := w.Write(buf); err != nil {
		log.Errorf("encode footer failed: %s", err.Error())
		return fmt.Errorf("encode footer failed: %w", err)
	}

	return nil
}
