package sstable

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/memtable"
)

func TestSSTableManagerCompaction(t *testing.T) {
	mgr := NewSSTableManager()
	tmp := t.TempDir()

	// 1. 创建 Level0 文件
	var oldFiles []string
	for i := 0; i < 5; i++ {
		mem := memtable.NewMemTable(uint64(i+1), tmp)
		key := "key" + strconv.Itoa(i)
		val := "val" + strconv.Itoa(i)
		err := mem.Insert(kv.KeyValuePair{Key: kv.Key(key), Value: []byte(val)})
		assert.NoError(t, err)

		imem := memtable.NewIMemTable(mem)
		sst := BuildSSTableFromIMemTable(imem)
		sst.level = 0 // 明确指定 Level0

		oldFiles = append(oldFiles, sst.FilePath())
		// 添加到管理器
		err = mgr.addNewSSTables([]*SSTable{sst})
		assert.NoError(t, err)
		// 验证文件可读
		assert.FileExists(t, sst.FilePath())
	}

	// 2. 触发合并
	err := mgr.Compaction()
	assert.NoError(t, err, "compaction failed")

	// 3. 验证 Level0 文件被删除
	for _, f := range oldFiles {
		assert.NoFileExists(t, f, "old Level0 file not deleted: %s", f)
	}

	// 4. 验证 Level0 totalMap 清空
	assert.Empty(t, mgr.totalMap[0], "Level0 totalMap not empty")

	// 5. 验证 Level1 有新文件生成
	level1Files := mgr.totalMap[1]
	assert.True(t, len(level1Files) > 0, "no Level1 files generated")

	// 6. 验证 Level1 文件内容正确
	for _, f := range level1Files {
		sst := NewSSTable()
		err := sst.DecodeFrom(f)
		assert.NoError(t, err, "decode Level1 SSTable failed: %s", f)
		assert.NotEmpty(t, sst.DataBlock, "Level1 SSTable has empty DataBlocks: %s", f)

		// 验证数据完整性
		for i, value := range sst.DataBlock.Entries {
			expectedVal := "val" + strconv.Itoa(i)
			assert.Equal(t, expectedVal, string(value), "data mismatch in %s", f)
		}
	}
}
