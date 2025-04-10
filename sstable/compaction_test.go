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
	// 1. 创建临时目录
	tmp := t.TempDir()
	// 2. 创建 SSTable 管理器
	mgr := NewSSTableManager()

	// 对于 Level0，maxFileNumsInLevel(0)=2^(0+2)=4，
	// 为触发合并，生成 5 个 Level0 文件
	var oldFiles []string
	for i := 0; i < 5; i++ {
		// 创建一个内存表并写入单个 key-value
		mem := memtable.NewMemtable(uint64(i+1), "")
		// 构造不同的 key/value，这里用 i 来生成唯一字符串
		keyStr := "key" + strconv.Itoa(i)
		valStr := "val" + strconv.Itoa(i)
		err := mem.Insert(kv.KeyValuePair{Key: kv.Key(keyStr), Value: []byte(valStr)})
		assert.NoError(t, err)
		imem := memtable.NewIMemtable(mem)
		sst := BuildSSTableFromIMemtable(imem)
		sst.level = 0
		// 得到文件路径，确保放在临时目录中（例如生成目录 tmp/001/0-level/...）
		path := sstableFilePath(sst.id, 0, tmp)
		err = sst.EncodeTo(path)
		assert.NoError(t, err, "sstable should be encoded to file")
		sst.filePath = path
		oldFiles = append(oldFiles, path)
		// 模拟该文件目前在磁盘中
		mgr.addTable(sst)
		mgr.addNewFile(0, sst.FilePath())
	}

	// 3. 触发 Compaction
	err := mgr.Compaction()
	assert.NoError(t, err, "compaction should succeed")

	// 4. 检查 Level0 的老文件是否已从磁盘中删除
	for _, f := range oldFiles {
		_, err := os.Stat(f)
		assert.True(t, os.IsNotExist(err), "old file should be removed after compaction: %s", f)
	}

	// 5. 检查 Level0 的 diskMap 与 totalMap 均已清空
	assert.Empty(t, mgr.diskMap[0], "level0 diskMap should be empty after compaction")
	assert.Empty(t, mgr.totalMap[0], "level0 totalMap should be empty after compaction")

	// 6. 检查 Level1 中应有新生成的文件
	assert.True(t, len(mgr.totalMap[1]) > 0, "compaction should produce new files in level1")

	// 7. 对 Level1 中的每个新文件尝试解码，并确保包含数据块
	for _, f := range mgr.totalMap[1] {
		sst := NewSSTable()
		err := sst.DecodeFrom(f)
		assert.NoError(t, err)
		assert.NotEmpty(t, sst.DataBlocks)
	}
}
