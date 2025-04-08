package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDatabasePutGetDelete 测试 Put、Get 和 Delete 的功能
func TestDatabasePutGetDelete(t *testing.T) {
	// 创建临时目录作为测试数据库的数据目录
	tempDir := t.TempDir()
	// 使用临时目录作为数据库名称
	db := Open(tempDir)

	// 测试查询不存在的 key，预期返回 nil
	val, err := db.Get("nonexistent")
	assert.NoError(t, err)
	assert.Nil(t, val)

	// Put 操作，将 key1 写入 value1
	err = db.Put("key1", []byte("value1"))
	assert.NoError(t, err)

	// Get 操作，查询 key1，预期返回 value1
	val, err = db.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)

	// Delete 操作，删除 key1
	err = db.Delete("key1")
	assert.NoError(t, err)

	// Get 操作应返回 nil（key1 被删除）
	val, err = db.Get("key1")
	assert.NoError(t, err)
	assert.Nil(t, val)
}
