package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/database"
)

const (
	numPutOperations = 2000000
	numGetOperations = 1000
)

func main() {
	rand.Seed(time.Now().UnixNano())

	db := database.Open("testdb")

	// 1) 写 (Put) 性能测试
	kvMap := make(map[string][]byte, numPutOperations)

	startWrite := time.Now()
	for i := 0; i < numPutOperations; i++ {
		key := "k_" + strconv.Itoa(i) + "_" + randomString(4)
		val := []byte("v_" + strconv.Itoa(i) + "_" + randomString(8))

		if err := db.Put(key, val); err != nil {
			fmt.Printf("Put error: %v\n", err)
			return
		}
		kvMap[key] = val
	}
	elapsedWrite := time.Since(startWrite)
	fmt.Println("finish write process")

	opsPerSecWrite := float64(numPutOperations) / elapsedWrite.Seconds()
	avgLatencyWrite := float64(elapsedWrite.Nanoseconds()) / float64(numPutOperations)

	// 2) 读 (Get) 性能测试
	// 等待一小段时间，让后台异步创建 SSTable 的 goroutine 完成
	time.Sleep(200 * time.Millisecond)

	startRead := time.Now()
	for j := 0; j < numGetOperations; j++ {
		key, expectVal := pickRandomKey(kvMap)
		if key == "" {
			continue
		}
		gotVal, err := db.Get(key)
		if err != nil {
			fmt.Printf("Get error: %v\n", err)
			return
		}
		// 校验是否是最新值
		if !bytes.Equal(gotVal, expectVal) {
			fmt.Printf("value mismatch! key=%s, expect=%v, got=%v\n", key, expectVal, gotVal)
		}
	}
	elapsedRead := time.Since(startRead)

	opsPerSecRead := float64(numPutOperations) / elapsedRead.Seconds()
	avgLatencyRead := float64(elapsedRead.Nanoseconds()) / float64(numGetOperations)

	fmt.Println("==============================================")
	fmt.Printf(" 测试目录   : %s\n", config.GetRootPath())
	fmt.Printf(" 写入数量   : %d\n", numPutOperations)
	fmt.Printf(" 写入耗时   : %s\n", elapsedWrite)
	fmt.Printf(" 写 ops/s   : %.2f\n", opsPerSecWrite)
	fmt.Printf(" 写 ns/op   : %.2f\n", avgLatencyWrite)

	fmt.Printf(" 读取数量   : %d\n", numGetOperations)
	fmt.Printf(" 读取耗时   : %s\n", elapsedRead)
	fmt.Printf(" 读 ops/s   : %.2f\n", opsPerSecRead)
	fmt.Printf(" 读 ns/op   : %.2f\n", avgLatencyRead)
	fmt.Println("==============================================")

	time.Sleep(5 * time.Second)
}

// 生成随机字符串作为 key 或 value
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// 从 map 随机取一个元素
func pickRandomKey(kvMap map[string][]byte) (string, []byte) {
	if len(kvMap) == 0 {
		return "", nil
	}
	// 随机偏移
	idx := rand.Intn(len(kvMap))
	i := 0
	for k, v := range kvMap {
		if i == idx {
			return k, v
		}
		i++
	}
	return "", nil
}
