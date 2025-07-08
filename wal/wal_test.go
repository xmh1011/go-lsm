package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/wal"
)

func TestWALAppendAndRecover(t *testing.T) {
	tempDir := t.TempDir()

	// 创建 WAL 实例
	w, err := wal.NewWAL(1, tempDir)
	assert.NoError(t, err)

	// 准备写入的测试数据
	records := []kv.KeyValuePair{
		{Key: "k1", Value: []byte("v1")},
		{Key: "k2", Value: kv.DeletedValue},
		{Key: "k3", Value: []byte("v3")},
	}

	// 写入 WAL
	for _, r := range records {
		assert.NoError(t, w.Append(r))
	}
	assert.NoError(t, w.Sync())
	assert.NoError(t, w.Close())

	// 构建 WAL 路径
	walPath := filepath.Join(tempDir, "1.wal")

	// 读取 WAL 并验证
	var recovered []kv.KeyValuePair
	recoveredWAL, err := wal.Recover(walPath, func(pair kv.KeyValuePair) {
		recovered = append(recovered, pair)
	})
	assert.NoError(t, err)

	// 关闭 WAL 文件
	assert.NoError(t, recoveredWAL.Close())

	// 验证数据是否一致
	assert.Equal(t, records, recovered)

	// 删除 WAL 文件
	err = recoveredWAL.DeleteFile()
	assert.NoError(t, err)
	_, statErr := os.Stat(walPath)
	assert.True(t, os.IsNotExist(statErr), "WAL file should be deleted")
}
