package database

import (
	"github.com/xmh1011/go-lsm/kv"
)

type Iterator interface {
	Valid() bool

	Key() kv.Key

	Next()

	Seek(key kv.Key)

	SeekToLast()

	SeekToFirst()

	Close()
}
