package sstable

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/memtable"
)

func TestSSTableManagerCompaction(t *testing.T) {
	// 临时目录
	tmp := t.TempDir()
	// 创建 SSTable 管理器
	mgr := NewSSTableManager()

	// 写入 3 个 Level0 文件 (超过 limit)
	var oldFiles []string
	for i := 0; i < 3; i++ {
		mem := memtable.NewMemtable(uint64(i+1), "")
		_ = mem.Insert(kv.KeyValuePair{Key: kv.Key("key" + strconv.Itoa('a'+i)), Value: []byte("val" + strconv.Itoa('a'+i))})
		imem := memtable.NewIMemtable(mem)
		sst := BuildSSTableFromIMemtable(imem)
		sst.level = 0
		path := sstableFilePath(sst.id, 0, tmp)
		err := sst.EncodeTo(path)
		assert.NoError(t, err, "sstable should be encoded to file")
		sst.filePath = path
		oldFiles = append(oldFiles, path)
		mgr.DiskMap[0] = append(mgr.DiskMap[0], path)
		mgr.TotalMap[0] = append(mgr.TotalMap[0], path)
	}

	// 触发合并
	err := mgr.Compaction()
	assert.NoError(t, err, "compaction should succeed")

	// 确保 Level0 的文件都被删除
	for _, f := range oldFiles {
		_, err := os.Stat(f)
		assert.True(t, os.IsNotExist(err), "old file should be removed after compaction: %s", f)
	}

	// 确保 Level0 的映射被清空
	assert.Empty(t, mgr.DiskMap[0], "level0 DiskMap should be empty after compaction")
	assert.Empty(t, mgr.TotalMap[0], "level0 TotalMap should be empty after compaction")

	// Level1 中应有新文件
	assert.True(t, len(mgr.TotalMap[1]) > 0, "compaction should produce new files in level1")

	// 检查新文件能正确解码
	for _, f := range mgr.TotalMap[1] {
		sst := NewSSTable()
		assert.NoError(t, sst.DecodeFrom(f))
		assert.NotEmpty(t, sst.DataBlocks)
	}
}
