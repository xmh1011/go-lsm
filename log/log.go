package log

import (
	"io"
	"os"
	"strings"
	"sync"

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
	logger   *logrus.Logger
	levelMap = map[string]logrus.Level{
		"debug": logrus.DebugLevel,
		"info":  logrus.InfoLevel,
		"warn":  logrus.WarnLevel,
		"error": logrus.ErrorLevel,
	}
	Config *LoggerConfig
	once   sync.Once
)

// 默认日志文件前缀
const (
	defaultLogFilePrefix = "lsm"
)

// InitLogger 初始化日志模块
func InitLogger(configPath string) error {
	var err error
	once.Do(func() {
		// 初始化配置文件
		if configPath == "" {
			InitDefaultLogger()
			return
		}
		cfg, err := ini.Load(configPath)
		if err != nil {
			return
		}
		if err := cfg.MapTo(&Config); err != nil {
			return
		}

		// 设置日志级别
		level := logrus.InfoLevel
		if l, ok := levelMap[strings.ToLower(Config.Level)]; ok {
			level = l
		}
		logger.SetLevel(level)

		// 设置日志格式
		switch strings.ToLower(Config.FileFormat) {
		case "json":
			logger.SetFormatter(&logrus.JSONFormatter{
				TimestampFormat: Config.TimeFormat,
			})
		default:
			logger.SetFormatter(&logrus.TextFormatter{
				TimestampFormat: Config.TimeFormat,
				FullTimestamp:   true,
			})
		}

		// 如果没有指定日志目录，默认写到当前执行文件目录下
		if Config.Path == "" {
			Config.Path = util.GetCurrentDir()
		}
		if err := os.MkdirAll(Config.Path, 0755); err != nil {
			return
		}

		// 创建一个可轮转的 Writer
		rotatingWriter := newRotatingWriter(Config, defaultLogFilePrefix)

		// 输出目标：文件(可轮转) + 控制台
		logger.SetOutput(io.MultiWriter(rotatingWriter, os.Stdout))
	})

	return err
}

func InitDefaultLogger() {
	logger = logrus.New()
	// 设置日志级别
	logger.SetLevel(logrus.InfoLevel)

	// 设置日志格式
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	// 输出目标：文件(可轮转) + 控制台
	logger.SetOutput(os.Stdout)
}

// GetLogger 返回全局 logger 实例
func GetLogger() *logrus.Logger {
	if logger == nil {
		InitDefaultLogger()
	}
	return logger
}

func Info(args ...any) {
	GetLogger().Info(args...)
}

func Infof(format string, args ...any) {
	GetLogger().Infof(format, args...)
}

func Error(args ...any) {
	GetLogger().Error(args...)
}

func Errorf(format string, args ...any) {
	GetLogger().Errorf(format, args...)
}

func Debug(args ...any) {
	GetLogger().Debug(args...)
}

func Debugf(format string, args ...any) {
	GetLogger().Debugf(format, args...)
}

func Warn(args ...any) {
	GetLogger().Warn(args...)
}

func Warnf(format string, args ...any) {
	GetLogger().Warnf(format, args...)
}
