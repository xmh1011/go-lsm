package sstable

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/memtable"
	"github.com/xmh1011/go-lsm/sstable/block"
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
		sst.level = minSSTableLevel

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

	// 4. 验证 Level0 清空
	assert.Empty(t, mgr.totalMap[0], "Level0 totalMap not empty")

	// 5. 验证 Level1 有新文件
	level1Files := mgr.totalMap[1]
	assert.True(t, len(level1Files) > 0, "no Level1 files generated")

	// 6. 验证 Level1 文件内容
	for _, f := range level1Files {
		sst := NewSSTable()
		err := sst.DecodeFrom(f)
		assert.NoError(t, err, "decode Level1 SSTable failed: %s", f)
		assert.NotEmpty(t, sst.DataBlock, "Level1 SSTable has empty DataBlocks: %s", f)
	}
}

func TestAsyncCompaction(t *testing.T) {
	mgr := NewSSTableManager()

	// 1. 创建 Level1 文件（超过阈值）
	var level1Files []string
	var tables []*SSTable
	for i := 0; i < maxFileNumsInLevel(1)+1; i++ {
		sst := NewSSTableWithLevel(1)
		// 添加一些测试数据
		for j := 0; j < 10; j++ {
			key := "key" + strconv.Itoa(i*10+j)
			value := "value" + strconv.Itoa(i*10+j)
			sst.Add(&kv.KeyValuePair{
				Key:   kv.Key(key),
				Value: []byte(value),
			})
		}
		sst.Header = block.NewHeader("key0", kv.Key("key"+strconv.Itoa(i*10+9)))
		tables = append(tables, sst)
		level1Files = append(level1Files, sst.FilePath())
	}
	err := mgr.addNewSSTables(tables)
	assert.NoError(t, err, "add new SSTable failed")

	// 2. 触发异步合并
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mgr.asyncCompactLevel(1)
	}()
	wg.Wait()

	// 3. 等待合并完成或超时
	timeout := time.After(30 * time.Second)
	select {
	case <-timeout:
		t.Fatal("async compaction timeout")
	default:
		// 检查合并是否完成：Level1 文件数减少或 Level2 出现新文件
		if len(mgr.getFilesByLevel(1)) < len(level1Files) || len(mgr.getFilesByLevel(2)) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 4. 验证旧文件被删除
	for _, f := range level1Files[:maxFileNumsInLevel(1)] {
		assert.NoFileExists(t, f, "old Level1 file still exists: %s", f)
	}

	// 5. 验证新文件生成
	assert.True(t, len(mgr.getFilesByLevel(1)) > 0 || len(mgr.getFilesByLevel(2)) > 0)
}

func TestCompactionOnEmptyLevel0(t *testing.T) {
	mgr := NewSSTableManager()

	// 不添加任何 Level0 文件，直接压缩
	err := mgr.Compaction()
	assert.NoError(t, err, "compaction on empty level0 should not fail")

	// 确认 Level0 和 Level1 都是空的
	assert.Empty(t, mgr.totalMap[0])
	assert.Empty(t, mgr.totalMap[1])
}

func TestCompactLevelWithNoFiles(t *testing.T) {
	mgr := NewSSTableManager()

	err := mgr.compactLevel(minSSTableLevel) // Level0
	assert.NoError(t, err, "compacting empty level should not error")
}

func TestCompactionWithInvalidSSTable(t *testing.T) {
	mgr := NewSSTableManager()

	// 模拟一个无效的文件路径
	mgr.totalMap[minSSTableLevel] = []string{"1.sst", "2.sst", "3.sst"}

	err := mgr.Compaction()
	assert.Error(t, err, "compaction should fail on invalid SSTable file")
}

func TestRecursiveCompactionAcrossLevels(t *testing.T) {
	mgr := NewSSTableManager()

	// 构造 Level0 -> Level1 -> Level2 的合并路径
	for i := 0; i < 4; i++ {
		mem := memtable.NewMemTable(uint64(i+1), t.TempDir())
		_ = mem.Insert(kv.KeyValuePair{
			Key:   kv.Key(fmt.Sprintf("key-%d", i)),
			Value: []byte("val"),
		})
		imem := memtable.NewIMemTable(mem)
		sst := BuildSSTableFromIMemTable(imem)
		sst.level = minSSTableLevel
		_ = mgr.addNewSSTables([]*SSTable{sst})
	}

	// 伪造更多 Level1 文件以触发下一层压缩
	for i := 0; i < maxFileNumsInLevel(1); i++ {
		sst := NewSSTableWithLevel(1)
		sst.Header = block.NewHeader("keyA", "keyZ")
		sst.Add(&kv.KeyValuePair{Key: "keyA", Value: []byte("valueA")})
		sst.Add(&kv.KeyValuePair{Key: "keyZ", Value: []byte("valueZ")})
		_ = mgr.addNewSSTables([]*SSTable{sst})
	}

	// 压缩，应触发 Level 0 -> Level1 -> Level2
	err := mgr.Compaction()
	assert.NoError(t, err)
	assert.True(t, len(mgr.getFilesByLevel(2)) > 0, "should generate Level2 SSTables")
}
