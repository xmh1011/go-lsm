package block

import (
	"fmt"
	"io"
	"os"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
)

type Header struct {
	MinKey kv.Key
	MaxKey kv.Key
}

func NewHeader(minKey, maxKey kv.Key) *Header {
	return &Header{
		MinKey: minKey,
		MaxKey: maxKey,
	}
}

// EncodeTo 将Header编码到io.Writer中（小端存储）
func (h *Header) EncodeTo(w io.Writer) error {
	if _, err := h.MinKey.EncodeTo(w); err != nil {
		log.Errorf("encode min key failed: %s", err)
		return fmt.Errorf("encode min key: %w", err)
	}

	if _, err := h.MaxKey.EncodeTo(w); err != nil {
		log.Errorf("encode max key failed: %s", err)
		return fmt.Errorf("encode max key: %w", err)
	}

	return nil
}

// DecodeFrom 从文件解码Header（小端存储）
func (h *Header) DecodeFrom(file *os.File) error {
	if _, err := h.MinKey.DecodeFrom(file); err != nil {
		log.Errorf("decode min key failed: %s", err)
		return fmt.Errorf("decode min key: %w", err)
	}

	if _, err := h.MaxKey.DecodeFrom(file); err != nil {
		log.Errorf("decode max key failed: %s", err)
		return err
	}

	return nil
}
