package util

import (
	"fmt"
	"math"
	"path/filepath"
	"runtime"
)

// GetCurrentDir 返回当前可执行文件所在的目录
func GetCurrentDir() string {
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		return "." // fallback
	}
	return filepath.Dir(file)
}

// ExtractIDFromFileName 从文件路径中提取 id，此处假设文件名格式为 "{id}.xxx"。
func ExtractIDFromFileName(fileName string) (uint64, error) {
	var id uint64
	_, err := fmt.Sscanf(fileName, "%d", &id)
	return id, err
}

func ExtractID(fileName string) uint64 {
	id, _ := ExtractIDFromFileName(fileName)
	return id
}

// ExtractLevelFromFilePath 提取 sstable 的压缩等级
func ExtractLevelFromFilePath(filePath string) (int, error) {
	var level int
	_, err := fmt.Sscanf(filepath.Base(filepath.Dir(filePath)), "%d", &level)
	return level, err
}

func GetOldestSSTableFile(files []string) string {
	var ans string
	var minID uint64
	minID = math.MaxUint64
	for _, file := range files {
		id, _ := ExtractIDFromFileName(filepath.Base(file))
		if id < minID {
			minID = id
			ans = file
		}
	}
	return ans
}
