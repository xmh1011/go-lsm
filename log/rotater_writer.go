package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// rotatingWriter 实现自定义日志轮转
type rotatingWriter struct {
	sync.Mutex

	// 配置
	dir        string        // 日志目录
	filePrefix string        // 日志文件名前缀
	maxAge     time.Duration // 保留天数(转为 time.Duration 方便计算)
	rotateSize int64         // 字节数, 用于大小切割
	rotateTime time.Duration // 小时 -> 转为 Duration
	// 当前文件信息
	file        *os.File  // 当前打开的日志文件
	createTime  time.Time // 当前文件创建时间
	currentSize int64     // 当前文件写入的字节总数
}

// newRotatingWriter 创建一个可轮转的 writer
func newRotatingWriter(cfg *LoggerConfig, prefix string) *rotatingWriter {
	w := &rotatingWriter{
		dir:        cfg.Path,
		filePrefix: prefix,
		maxAge:     time.Duration(cfg.MaxAge) * 24 * time.Hour,
		rotateSize: int64(cfg.RotateSize * 1024 * 1024), // MB -> bytes
		rotateTime: time.Duration(cfg.RotateTime) * time.Hour,
	}
	// 启动时先创建一个日志文件
	_ = w.rotateFile()
	return w
}

// Write 实现 io.Writer 接口
func (w *rotatingWriter) Write(p []byte) (n int, err error) {
	w.Lock()
	defer w.Unlock()

	if w.needRotate(len(p)) {
		if err := w.rotateFile(); err != nil {
			return 0, err
		}
	}

	n, err = w.file.Write(p)
	if err != nil {
		return n, err
	}

	// 更新当前文件写入大小
	w.currentSize += int64(n)
	return n, nil
}

// needRotate 判断是否需要切割
func (w *rotatingWriter) needRotate(incoming int) bool {
	// 1. 按大小轮转：如果(当前大小 + 即将写入) > rotateSize
	if w.rotateSize > 0 && (w.currentSize+int64(incoming)) > w.rotateSize {
		return true
	}

	// 2. 按时间轮转：如果已经超出 rotateTime
	if w.rotateTime > 0 && time.Since(w.createTime) >= w.rotateTime {
		return true
	}

	return false
}

// rotateFile 关闭旧文件并创建新文件
func (w *rotatingWriter) rotateFile() error {
	// 先关闭旧文件
	if w.file != nil {
		_ = w.file.Close()
	}

	// 生成新的文件名
	filename := w.makeLogFileName()
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("无法创建日志文件: %v", err)
	}

	w.file = file
	w.createTime = time.Now()
	w.currentSize = 0

	// 切完之后立即清理旧日志
	w.cleanupOldLogs()

	return nil
}

// makeLogFileName 根据前缀 + 时间戳组合新文件名
func (w *rotatingWriter) makeLogFileName() string {
	t := time.Now().Format("20060102_150405")
	fileName := fmt.Sprintf("%s_%s.log", w.filePrefix, t)
	return filepath.Join(w.dir, fileName)
}

// cleanupOldLogs 删除过期的日志文件
func (w *rotatingWriter) cleanupOldLogs() {
	// 如果没配置 maxAge 或者为0，表示不删除
	if w.maxAge <= 0 {
		return
	}

	files, err := os.ReadDir(w.dir)
	if err != nil {
		return
	}

	cutoff := time.Now().Add(-w.maxAge)

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		// 判断是否是本前缀生成的日志文件
		if !isOurLogFile(f.Name(), w.filePrefix) {
			continue
		}

		fp := filepath.Join(w.dir, f.Name())
		info, err := f.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			_ = os.Remove(fp)
		}
	}
}

// isOurLogFile 判断是否是本程序生成的日志文件
func isOurLogFile(filename, prefix string) bool {
	// 例如：lsm_20250402_150405.log
	// 判断前缀 + '_' 是否匹配
	return len(filename) > len(prefix) &&
		filepath.Ext(filename) == ".log" &&
		strings.HasPrefix(filename, prefix+"_")
}
