package block

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFooterEncodeDecode(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "footer_test.sst")

	f, err := os.Create(path)
	assert.NoError(t, err)
	defer f.Close()

	original := Footer{
		MetaIndexHandle: Handle{Offset: 100, Size: 200},
		IndexHandle:     Handle{Offset: 300, Size: 400},
		MagicNumber:     0xdeadbeefcafebabe,
	}

	buf := &bytes.Buffer{}
	err = original.EncodeTo(buf)
	assert.NoError(t, err)
	assert.Equal(t, 48, buf.Len(), "footer encoded length should be 48 bytes")

	_, err = f.Write(buf.Bytes())
	assert.NoError(t, err)
	assert.NoError(t, f.Sync())
	assert.NoError(t, f.Close())

	f, err = os.Open(path)
	assert.NoError(t, err)
	defer f.Close()

	var decoded Footer
	err = decoded.DecodeFrom(f)
	assert.NoError(t, err)

	assert.Equal(t, original.MetaIndexHandle, decoded.MetaIndexHandle, "MetaIndexHandle mismatch")
	assert.Equal(t, original.IndexHandle, decoded.IndexHandle, "IndexHandle mismatch")
	assert.Equal(t, original.MagicNumber, decoded.MagicNumber, "MagicNumber mismatch")
}

func TestFooterDecodeInvalidSize(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 30)) // less than footerSize
	tmpFile, err := os.CreateTemp("", "footer_invalid")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	_, _ = tmpFile.Write(buf.Bytes())
	_ = tmpFile.Sync()
	_ = tmpFile.Close()

	f, err := os.Open(tmpFile.Name())
	assert.NoError(t, err)
	defer f.Close()

	var footer Footer
	err = footer.DecodeFrom(f)
	assert.Error(t, err, "should fail if file size is too small")
}

func TestFooterMagicNumberEncoding(t *testing.T) {
	buf := &bytes.Buffer{}
	magic := uint64(0x1122334455667788)
	err := binary.Write(buf, binary.LittleEndian, magic)
	assert.NoError(t, err)
	assert.Equal(t, []byte{
		0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
	}, buf.Bytes(), "should be little-endian")
}
