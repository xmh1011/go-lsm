package database

import (
	"strconv"
	"testing"
	"time"
)

// BenchmarkPut 测试 Put 操作的吞吐量和平均时延
func BenchmarkPut(b *testing.B) {
	// 使用临时目录作为数据库存储目录
	dir := b.TempDir()

	db := Open(dir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key_" + strconv.Itoa(i)
		value := []byte("value_" + strconv.Itoa(i))
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
		// 注意：Put 内部是异步创建 SSTable，实际写入延时不计入此处
	}
	b.StopTimer()
}

// BenchmarkGet 测试 Get 操作的吞吐量和平均时延
func BenchmarkGet(b *testing.B) {
	// 使用临时目录作为数据库存储目录
	dir := b.TempDir()

	db := Open(dir)

	// 预先写入固定数量的 key-value，后续循环中周期性获取这些 key 的数据
	keyCount := 100000
	for i := 0; i < keyCount; i++ {
		key := "key_" + strconv.Itoa(i)
		value := []byte("value_" + strconv.Itoa(i))
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}
	// 等待后台异步操作完成（如 SSTable 创建等）
	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 轮询选择预写入的 key
		idx := i % keyCount
		key := "key_" + strconv.Itoa(idx)
		val, err := db.Get(key)
		if err != nil {
			b.Fatalf("Get error: %v", err)
		}
		expected := "value_" + strconv.Itoa(idx)
		if string(val) != expected {
			b.Fatalf("Get error: expected %s, got %s", expected, string(val))
		}
	}
	b.StopTimer()
}

// BenchmarkDelete 测试 Delete 操作的吞吐量和平均时延。
func BenchmarkDelete(b *testing.B) {
	// 使用临时目录作为数据库存储目录
	dir := b.TempDir()

	db := Open(dir)

	keyCount := 100000
	for i := 0; i < keyCount; i++ {
		key := "key_" + strconv.Itoa(i)
		value := []byte("value_" + strconv.Itoa(i))
		if err := db.Put(key, value); err != nil {
			b.Fatalf("Put error: %v", err)
		}
	}
	// 等待后台异步操作完成
	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 循环中删除一个存在的 key，然后重新插入保证下次删除时是最新数据
		idx := i % keyCount
		key := "key_" + strconv.Itoa(idx)
		if err := db.Delete(key); err != nil {
			b.Fatalf("Delete error: %v", err)
		}
		// 重新插入 key ，新 value 保证最新性
		newValue := []byte("new_value_" + strconv.Itoa(i))
		if err := db.Put(key, newValue); err != nil {
			b.Fatalf("Re-Put error: %v", err)
		}
	}
	b.StopTimer()
}
