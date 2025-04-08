package database

import (
	"github.com/xmh1011/go-lsm/kv"
)

type Iterator interface {
	Valid() bool

	Key() kv.Key

	Value() kv.Value

	Pair() *kv.KeyValuePair

	Next()

	Seek(key kv.Key)

	SeekToLast()

	SeekToFirst()

	Close()
}
