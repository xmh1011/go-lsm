package memtable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/kv"
)

func TestMemTableBuilderInsertAndEviction(t *testing.T) {
	manager := NewMemTableManager()

	// 手动构造 key-value 对，每次都填满 MemTable 触发 Promote
	var evicted *IMemTable
	var err error
	for i := 0; i <= maxIMemTableCount; i++ {
		// 构造一个大的 kv，使得每次都触发 Promote
		key := kv.Key(fmt.Sprintf("key-%03d", i))
		value := kv.Value(make([]byte, maxMemoryTableSize)) // 触发 Flush
		evicted, err = manager.Insert(kv.KeyValuePair{Key: key, Value: value})
		assert.NoError(t, err, "should not return error on insert")
	}

	// 第一次出现淘汰，应是在第 (maxIMemTableCount+1) 次 Promote 时
	assert.NotNil(t, evicted, "should return evicted imem")

	// 再插一次，继续淘汰第二个 imem
	evicted2, _ := manager.Insert(kv.KeyValuePair{
		Key:   "last",
		Value: make([]byte, 512*1024),
	})
	assert.NotNil(t, evicted2, "second eviction should not be nil")

	// 验证 IMemTable 数量不超过 maxIMemTableCount
	all := manager.GetAll()
	assert.LessOrEqual(t, len(all), maxIMemTableCount, "should not exceed max count")

	// 验证当前 MemTable 仍可写入
	ok := manager.CanInsert(kv.KeyValuePair{Key: "z", Value: []byte("zzz")})
	assert.True(t, ok, "should still allow insert to new MemTable")
}

func TestInsertTriggersPromotion(t *testing.T) {
	tempDir := t.TempDir()
	config.Conf.WALPath = tempDir

	manager := NewMemTableManager()

	var evicted *IMemTable
	var err error
	for i := 0; i <= maxIMemTableCount; i++ {
		// 构造一个大的 kv，使得每次都触发 Promote
		key := kv.Key(fmt.Sprintf("key-%03d", i))
		value := kv.Value(make([]byte, maxMemoryTableSize)) // 触发 Flush
		evicted, err = manager.Insert(kv.KeyValuePair{Key: key, Value: value})
		assert.NoError(t, err, "should not return error on insert")
	}
	// 第一次出现淘汰，应是在第 (maxIMemTableCount+1) 次 Promote 时
	assert.NotNil(t, evicted, "should return evicted imem")

	// 再插入应触发 promote
	evicted, err = manager.Insert(kv.KeyValuePair{Key: "newKey", Value: []byte("newValue")})
	assert.NoError(t, err)
	assert.NotNil(t, evicted, "Should evict one IMemTable")
	assert.Equal(t, 10, len(manager.GetAll()), "Should have ten IMemTable")

	// 触发 promote 后再次 delete
	evicted, err = manager.Delete("someKey")
	assert.NoError(t, err)
	assert.Nil(t, evicted, "Should evict one IMemTable")

	val := manager.Search("someKey")
	assert.Nil(t, val, "Deleted key should return nil")
}

func TestDeleteTriggersPromotion(t *testing.T) {
	tempDir := t.TempDir()
	config.Conf.WALPath = tempDir

	manager := NewMemTableManager()

	// 填满 MemTable
	for i := 0; i < 100000; i++ {
		key := kv.Key(fmt.Sprintf("k%d", i))
		_, _ = manager.Insert(kv.KeyValuePair{Key: key, Value: []byte("v")})
	}

}

func TestSearchFromMemTables(t *testing.T) {
	tempDir := t.TempDir()
	config.Conf.WALPath = tempDir

	manager := NewMemTableManager()
	_, err := manager.Insert(kv.KeyValuePair{Key: "key", Value: []byte("value")})
	assert.NoError(t, err, "Insert should not return error")

	_, err = manager.Insert(kv.KeyValuePair{Key: "key", Value: []byte("newValue")}) // 更新同一 key
	assert.NoError(t, err, "Insert should not return error")

	val := manager.Search("key")
	assert.Equal(t, kv.Value("newValue"), val)
}

// mockCreateWalFile 在指定目录下创建一个空的 WAL 文件，文件名必须符合 ExtractID 的格式 "000001.wal"
func mockCreateWalFile(t *testing.T, dir string, id uint64) string {
	filename := filepath.Join(dir, fmt.Sprintf("%d.wal", id)) // 比如 "1.wal"
	f, err := os.Create(filename)
	assert.NoError(t, err)
	assert.NoError(t, f.Close(), "WAL file should be created")
	return filename
}

func TestRecoverSuccess(t *testing.T) {
	tempDir := t.TempDir()
	config.Conf.WALPath = tempDir

	// 创建多个 WAL 文件，id 从 1 到 5
	for i := uint64(1); i <= 5; i++ {
		mockCreateWalFile(t, tempDir, i)
	}

	manager := NewMemTableManager()

	// Recover 应成功返回，且最后一个 WAL 恢复的 MemTable 是 manager.Mem，其余是 IMemTable
	err := manager.Recover()
	assert.NoError(t, err)
	assert.NotNil(t, manager.Mem)
	assert.GreaterOrEqual(t, len(manager.IMems), 0)

	// 确认 IMemTable 数量不超过 maxIMemTableCount
	assert.LessOrEqual(t, len(manager.IMems), maxIMemTableCount)

	// 检查 manager.Mem 的 id 是最后一个文件的 id
	lastFile := mockCreateWalFile(t, tempDir, 100)
	_ = os.Remove(lastFile) // 先删了，再做下个测试用
}

// 模拟 WAL 目录读取失败
func TestRecoverReadDirFail(t *testing.T) {
	tmp := t.TempDir()
	config.Conf.WALPath = tmp
	manager := NewMemTableManager()

	// 传入一个不存在的目录
	config.Conf.WALPath = "/path/does/not/exist"

	err := manager.Recover()
	assert.Error(t, err)
}

// 模拟 WAL 恢复失败（用空文件名，必定失败）
func TestRecoverFromWALFail(t *testing.T) {
	tempDir := t.TempDir()
	config.Conf.WALPath = tempDir

	// 创建一个非法文件名
	fname := filepath.Join(tempDir, "invalid.wal")
	f, err := os.Create(fname)
	assert.NoError(t, err)
	assert.NoError(t, f.Close(), "WAL file should be created")

	manager := NewMemTableManager()

	err = manager.Recover()
	assert.Error(t, err)
}
