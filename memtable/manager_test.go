package memtable

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

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
		value := kv.Value(make([]byte, maxMemoryTableSize)) // 每个 value 占 0.5MB，2次就超过 1MB，触发 Flush
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
