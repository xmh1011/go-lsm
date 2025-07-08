package log

import (
	"io"
	"os"
	"strings"

	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"

	"github.com/xmh1011/go-lsm/util"
)

type LoggerConfig struct {
	Level      string `ini:"level"`           // 日志级别: debug/info/warn/error
	Path       string `ini:"log_path"`        // 日志目录
	MaxAge     int64  `ini:"log_max_age"`     // 保留天数
	RotateSize int64  `ini:"log_rotate_size"` // 单位：MB
	RotateTime int64  `ini:"log_rotate_time"` // 单位：小时
	FileFormat string `ini:"log_file_format"` // text/json
	TimeFormat string `ini:"log_time_format"` // 如 "2006-01-02 15:04:05"
}

var (
	// 包级日志器（通过 init() 初始化）
	logger *logrus.Logger

	// 配置映射
	levelMap = map[string]logrus.Level{
		"debug": logrus.DebugLevel,
		"info":  logrus.InfoLevel,
		"warn":  logrus.WarnLevel,
		"error": logrus.ErrorLevel,
	}

	// 默认配置
	defaultConfig = &LoggerConfig{
		Level:      "info",
		Path:       util.GetCurrentDir(),
		FileFormat: "text",
		TimeFormat: "2006-01-02 15:04:05",
	}
)

const defaultLogFilePrefix = "lsm"

// init 在导入包时自动初始化默认日志器
func init() {
	logger = logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	logger.SetOutput(os.Stdout)
}

// InitLogger 从配置文件初始化日志器（可选）
func InitLogger(configPath string) error {
	if configPath == "" {
		return nil // 使用默认配置
	}

	// 加载配置文件
	cfg, err := ini.Load(configPath)
	if err != nil {
		return err
	}

	config := defaultConfig // 基于默认配置覆盖
	if err := cfg.MapTo(config); err != nil {
		return err
	}

	// 设置日志级别
	if level, ok := levelMap[strings.ToLower(config.Level)]; ok {
		logger.SetLevel(level)
	}

	// 设置日志格式
	switch strings.ToLower(config.FileFormat) {
	case "json":
		logger.SetFormatter(&logrus.JSONFormatter{TimestampFormat: config.TimeFormat})
	default:
		logger.SetFormatter(&logrus.TextFormatter{
			TimestampFormat: config.TimeFormat,
			FullTimestamp:   true,
		})
	}

	// 确保日志目录存在
	if err := os.MkdirAll(config.Path, 0755); err != nil {
		return err
	}

	// 设置输出（文件轮转 + 控制台）
	rotatingWriter := newRotatingWriter(config, defaultLogFilePrefix)
	logger.SetOutput(io.MultiWriter(rotatingWriter, os.Stdout))

	return nil
}

func Info(args ...any) {
	logger.Info(args...)
}

func Infof(format string, args ...any) {
	logger.Infof(format, args...)
}

func Error(args ...any) {
	logger.Error(args...)
}

func Errorf(format string, args ...any) {
	logger.Errorf(format, args...)
}

func Debug(args ...any) {
	logger.Debug(args...)
}

func Debugf(format string, args ...any) {
	logger.Debugf(format, args...)
}

func Warn(args ...any) {
	logger.Warn(args...)
}

func Warnf(format string, args ...any) {
	logger.Warnf(format, args...)
}
