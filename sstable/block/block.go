// Definitions for blocks of SSTable.

package block

import (
	"fmt"
	"io"

	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/util"
)

// Handle 用于描述一个数据块在文件中的位置及大小。
// SSTable 中，IndexBlock、FilterBlock 和 Footer 均依赖于 Handle 记录数据块的偏移量和大小。
type Handle struct {
	Offset uint64 // 数据块在文件中的起始偏移量
	Size   uint64 // 数据块的大小（字节数）
}

// DecodeFrom 使用 uvarint 解码填充 Handle
func (h *Handle) DecodeFrom(r io.Reader) error {
	var err error
	if h.Offset, err = util.ReadUvarint(r); err != nil {
		log.Errorf("read offset failed: %s", err.Error())
		return fmt.Errorf("read offset failed: %w", err)
	}
	if h.Size, err = util.ReadUvarint(r); err != nil {
		log.Errorf("read size failed: %s", err.Error())
		return fmt.Errorf("read size failed: %w", err)
	}
	return nil
}

// EncodeTo 将 Handle 使用 uvarint 编码写入到 io.Writer
func (h *Handle) EncodeTo(w io.Writer) error {
	if err := util.WriteUvarint(w, h.Offset); err != nil {
		log.Errorf("write offset failed: %s", err.Error())
		return fmt.Errorf("write offset failed: %w", err)
	}
	if err := util.WriteUvarint(w, h.Size); err != nil {
		log.Errorf("write size failed: %s", err.Error())
		return fmt.Errorf("write size failed: %w", err)
	}
	return nil
}
