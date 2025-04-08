package config

import (
	"log"
	"path/filepath"

	"github.com/go-ini/ini"

	"github.com/xmh1011/go-lsm/util"
)

var Conf Config

const (
	walFileDirectory  = "wal"
	sstableDirectory  = "sstable"
	defaultConfigFile = "config.ini" // 默认配置文件名
)

type Config struct {
	RootPath    string `ini:"root_path"`
	WALPath     string `ini:"wal_path"`
	SSTablePath string `ini:"sstable_path"`
}

func init() {
	// 获取当前工作目录下的 config.ini
	configPath := filepath.Join(util.GetCurrentDir(), defaultConfigFile)

	// 解析配置文件
	cfg, err := ini.Load(configPath)
	if err != nil {
		log.Printf("[config] 未找到配置文件 %s，使用默认路径: %v", configPath, err)
		return
	}

	// 加载配置到 Conf 结构体中
	err = cfg.MapTo(&Conf)
	if err != nil {
		log.Printf("[config] 解析配置文件失败: %v", err)
	}
}

func GetWALPath() string {
	if Conf.WALPath != "" {
		return Conf.WALPath
	}
	return filepath.Join(util.GetCurrentDir(), walFileDirectory)
}

func GetSSTablePath() string {
	if Conf.SSTablePath != "" {
		return Conf.SSTablePath
	}
	return filepath.Join(util.GetCurrentDir(), sstableDirectory)
}

func GetRootPath() string {
	if Conf.RootPath != "" {
		return Conf.RootPath
	}
	return util.GetCurrentDir()
}
