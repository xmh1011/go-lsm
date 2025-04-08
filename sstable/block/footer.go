/*
Footer structure:
┌─────────────────────────────┬─────────────────────────┬─────────────┐
│ MetaIndex Handle (可变)      │ Index Handle (可变)      │ MagicNumber │
├─────────────────────────────┴─────────────────────────┼─────────────┤
│      Handle 区总共 40 字节（不足时 padding 填充）         │  固定 8字节  │
└───────────────────────────────────────────────────────┴─────────────┘
             共计固定长度：48 字节
*/

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/xmh1011/go-lsm/log"
)

const (
	footerSize = 48 // Footer 固定 48 字节
)

// Footer 位于 SSTable 文件尾部，固定占用 48 字节。
// 它记录了 FilterBlock 和 IndexBlock 的 Handle 信息，
// Handle 采用不定长编码存储（最多 10 字节 * 4 = 40 字节），
// Footer 最后 8 字节为 magic number，用于校验文件的正确性。
// 为保证 Footer 长度固定，若 Handle 编码不足 40 字节，会通过 padding 补齐。
type Footer struct {
	MetaIndexHandle Handle // FilterBlock 的 Handle
	IndexHandle     Handle // IndexBlock 的 Handle

	// 在序列化时，上述两个 Handle 将采用不定长编码后，总长度补齐至 40 字节
	MagicNumber uint64 // 固定 8 字节的 magic number，用于文件校验
}

// DecodeFrom 从 48 字节的 footerData 中解析出 Footer 结构。
// footerData 的最后 8 字节为 magic number，其余 40 字节存储两个 Handle 的不定长编码，
// 中间可能包含 padding（补齐到 40 字节）。
// DecodeFrom 从 SSTable 文件尾部解析 Footer，包括两个 Handle 和 MagicNumber。
func (f *Footer) DecodeFrom(file *os.File) error {
	stat, err := file.Stat()
	if err != nil {
		log.Errorf("stat file %s error: %s", file.Name(), err.Error())
		return fmt.Errorf("stat file error: %w", err)
	}
	if stat.Size() < footerSize {
		log.Errorf("file %s too small to contain footer", file.Name())
		return fmt.Errorf("file too small to contain footer")
	}

	// 1. 定位到 footer 起始位置
	footerOffset := stat.Size() - footerSize
	if _, err := file.Seek(footerOffset, io.SeekStart); err != nil {
		log.Errorf("seek footer error: %s", err.Error())
		return fmt.Errorf("seek footer error: %w", err)
	}

	// 2. 读取完整 footer 内容（48 字节）
	buf := make([]byte, footerSize)
	if _, err := io.ReadFull(file, buf); err != nil {
		log.Errorf("read footer error: %s", err.Error())
		return fmt.Errorf("read footer error: %w", err)
	}

	// 3. 读取最后 8 字节 MagicNumber
	f.MagicNumber = binary.LittleEndian.Uint64(buf[40:48])

	// 4. 解析前 40 字节中的两个 Handle（不定长 + padding）
	reader := bytes.NewReader(buf[:40])

	// 解析 MetaIndexHandle
	if err := f.MetaIndexHandle.DecodeFrom(reader); err != nil {
		log.Errorf("decode meta index handle error: %s", err.Error())
		return fmt.Errorf("decode meta index handle: %w", err)
	}

	// 解析 IndexHandle（剩余部分）
	if err := f.IndexHandle.DecodeFrom(reader); err != nil {
		log.Errorf("decode index handle error: %s", err.Error())
		return fmt.Errorf("decode index handle: %w", err)
	}

	return nil
}

// EncodeTo 将 Footer 写入给定 writer 中，格式固定为 48 字节。
// 前 40 字节存储两个 Handle 的变长编码（不足用 0x00 padding 补齐），
// 最后 8 字节写入 MagicNumber。
func (f *Footer) EncodeTo(w io.Writer) error {
	var handleBuf bytes.Buffer

	// 编码 MetaIndexHandle
	if err := f.MetaIndexHandle.EncodeTo(&handleBuf); err != nil {
		log.Errorf("encode meta index handle failed: %s", err.Error())
		return fmt.Errorf("encode meta index handle failed: %w", err)
	}

	// 编码 IndexHandle
	if err := f.IndexHandle.EncodeTo(&handleBuf); err != nil {
		log.Errorf("encode index handle failed: %s", err.Error())
		return fmt.Errorf("encode index handle failed: %w", err)
	}

	// 判断编码后的总长度
	handleData := handleBuf.Bytes()
	if len(handleData) > 40 {
		log.Errorf("encoded handle data too long: %d > 40", len(handleData))
		return fmt.Errorf("encoded handle data too long: %d > 40", len(handleData))
	}

	// 写入 handle 区（前 40 字节），不足部分 padding
	if _, err := w.Write(handleData); err != nil {
		log.Errorf("write handles failed: %s", err.Error())
		return fmt.Errorf("write handles failed: %w", err)
	}
	if padding := 40 - len(handleData); padding > 0 {
		if _, err := w.Write(make([]byte, padding)); err != nil {
			log.Errorf("write padding failed: %s", err.Error())
			return fmt.Errorf("write padding failed: %w", err)
		}
	}

	// 写入 MagicNumber（最后 8 字节）
	if err := binary.Write(w, binary.LittleEndian, f.MagicNumber); err != nil {
		log.Errorf("write magic number failed: %s", err.Error())
		return fmt.Errorf("write magic number failed: %w", err)
	}

	return nil
}
