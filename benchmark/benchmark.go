package main

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/database"
)

const (
	numPutOperations = 1000000
	numGetOperations = 1000
	numRounds        = 5
)

func main() {
	var totalWriteTime time.Duration
	var totalReadTime time.Duration
	var totalWriteOps float64
	var totalReadOps float64
	var totalWriteNsOp float64
	var totalReadNsOp float64

	db := database.Open("testDB")
	kvMap := make(map[string][]byte, numPutOperations*numRounds)
	keys := make([]string, numPutOperations*numRounds)

	for t := 0; t < numRounds; t++ {
		// 写入测试
		startWrite := time.Now()
		for i := 0; i < numPutOperations; i++ {
			key := "k_" + strconv.Itoa(i) + "_" + randomString(randIntInRange(1, 10))
			keys[t*numPutOperations+i] = key
			val := []byte("v_" + strconv.Itoa(i) + "_" + randomString(randIntInRange(2, 20)))
			if err := db.Put(key, val); err != nil {
				fmt.Printf("Put error: %v\n", err)
				return
			}
			kvMap[key] = val
		}
		elapsedWrite := time.Since(startWrite)
		opsPerSecWrite := float64(numPutOperations) / elapsedWrite.Seconds()
		avgLatencyWrite := float64(elapsedWrite.Nanoseconds()) / float64(numPutOperations)

		// 读取测试
		time.Sleep(200 * time.Millisecond)

		startRead := time.Now()
		for j := 0; j < numGetOperations; j++ {
			key, expectVal := pickRandomKey(kvMap, keys)
			if key == "" {
				continue
			}
			gotVal, err := db.Get(key)
			if err != nil {
				fmt.Printf("Get error: %v\n", err)
				return
			}
			if !bytes.Equal(gotVal, expectVal) {
				fmt.Printf("value mismatch! key=%s, expect=%v, got=%v\n", key, expectVal, gotVal)
			}
		}
		elapsedRead := time.Since(startRead)
		opsPerSecRead := float64(numGetOperations) / elapsedRead.Seconds()
		avgLatencyRead := float64(elapsedRead.Nanoseconds()) / float64(numGetOperations)

		// 累加
		totalWriteTime += elapsedWrite
		totalReadTime += elapsedRead
		totalWriteOps += opsPerSecWrite
		totalReadOps += opsPerSecRead
		totalWriteNsOp += avgLatencyWrite
		totalReadNsOp += avgLatencyRead
	}

	// 平均输出
	fmt.Println("==============================================")
	fmt.Printf(" 测试目录   : %s\n", config.GetRootPath())
	fmt.Printf(" 循环轮数   : %d\n", numRounds)
	fmt.Printf(" 写入总数   : %d\n", numPutOperations*numRounds)
	fmt.Printf(" 写入耗时   : %s (平均)\n", totalWriteTime/time.Duration(numRounds))
	fmt.Printf(" 写 ops/s  : %.2f (平均)\n", totalWriteOps/float64(numRounds))
	fmt.Printf(" 写 ns/op  : %.2f (平均)\n", totalWriteNsOp/float64(numRounds))
	fmt.Printf(" 读取总数   : %d\n", numGetOperations*numRounds)
	fmt.Printf(" 读取耗时   : %s (平均)\n", totalReadTime/time.Duration(numRounds))
	fmt.Printf(" 读 ops/s  : %.2f (平均)\n", totalReadOps/float64(numRounds))
	fmt.Printf(" 读 ns/op  : %.2f (平均)\n", totalReadNsOp/float64(numRounds))
	fmt.Println("==============================================")
}

// 返回 [x, y] 范围的随机整数
func randIntInRange(x, y int) int {
	if y < x {
		x, y = y, x
	}
	return x + rand.IntN(y-x+1)
}

// 随机字符串
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

// 从 map 随机取一个元素
func pickRandomKey(kvMap map[string][]byte, keys []string) (string, []byte) {
	if len(kvMap) == 0 {
		return "", nil
	}
	idx := rand.IntN(len(keys))
	key := keys[idx]
	expectVal, ok := kvMap[key]
	if !ok {
		return "", nil
	}
	return key, expectVal
}
