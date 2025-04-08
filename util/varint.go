// uvarint 是 Go 标准库 encoding/binary 提供的一种变长整数编码方式，
// 用于压缩小整数，在 SSTable 中广泛用于节省空间。

package util

import (
	"encoding/binary"
	"fmt"
	"io"
)

func WriteUvarint(w io.Writer, x uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], x)
	_, err := w.Write(buf[:n])
	return err
}

func ReadUvarint(r io.Reader) (uint64, error) {
	var buf [1]byte
	var x uint64
	var s uint
	for i := 0; ; i++ {
		if i >= binary.MaxVarintLen64 {
			return 0, fmt.Errorf("uvarint too long")
		}
		if _, err := r.Read(buf[:]); err != nil {
			return 0, err
		}
		b := buf[0]
		if b < 0x80 {
			if i == binary.MaxVarintLen64-1 && b > 1 {
				return 0, fmt.Errorf("uvarint overflow")
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}
