package util

import (
	"sync/atomic"
)

type IDGenerator struct {
	id atomic.Uint64
}

var IDGen *IDGenerator

func init() {
	IDGen = NewIDGenerator()
}

func NewIDGenerator() *IDGenerator {
	return &IDGenerator{}
}

func (gen *IDGenerator) Next() uint64 {
	return gen.id.Add(1)
}
