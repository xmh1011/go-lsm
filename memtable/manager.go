package memtable

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/xmh1011/go-lsm/config"
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/util"
)

const (
	maxIMemTableCount = 10
)

var idGenerator atomic.Uint64

type Manager struct {
	mu    sync.RWMutex
	Mem   *MemTable
	IMems []*IMemTable
}

func NewMemTableManager() *Manager {
	return &Manager{
		Mem:   NewMemTable(idGenerator.Add(1), config.Conf.WALPath),
		IMems: make([]*IMemTable, 0),
	}
}

func (m *Manager) Insert(pair kv.KeyValuePair) (*IMemTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Mem.CanInsert(pair) {
		if err := m.Mem.Insert(pair); err != nil {
			log.Errorf("insert memtable error: %s", err.Error())
			return nil, fmt.Errorf("insert memtable error: %w", err)
		}
		return nil, nil
	}

	evicted := m.promoteLocked()
	if err := m.Mem.Insert(pair); err != nil {
		log.Errorf("insert after promote error: %s", err.Error())
		return nil, fmt.Errorf("insert after promote error: %w", err)
	}

	return evicted, nil
}

func (m *Manager) Search(key kv.Key) kv.Value {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if value, ok := m.Mem.Search(key); ok {
		return value
	}
	for i := len(m.IMems) - 1; i >= 0; i-- {
		if value, ok := m.IMems[i].Search(key); ok {
			return value
		}
	}
	return nil
}

func (m *Manager) Delete(key kv.Key) (*IMemTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.Mem.Delete(key); err != nil {
		log.Errorf("delete memtable error: %s", err.Error())
		return nil, fmt.Errorf("delete memtable error: %w", err)
	}
	// 如果 memtable 中没有这个 key，但是说明有可能存在于内存中的 imemtable
	// 而 imemtable 是不可变的，所以这时候需要在 memtable 中插入一条 nil 记录
	pair := kv.KeyValuePair{
		Key:   key,
		Value: kv.DeletedValue,
	}
	if m.Mem.CanInsert(pair) {
		if err := m.Mem.Insert(pair); err != nil {
			log.Errorf("insert memtable error: %s", err.Error())
			return nil, fmt.Errorf("insert memtable error: %w", err)
		}
		return nil, nil
	}

	evicted := m.promoteLocked()
	if err := m.Mem.Insert(pair); err != nil {
		log.Errorf("insert after promote error: %s", err.Error())
		return nil, fmt.Errorf("insert after promote error: %w", err)
	}

	return evicted, nil
}

func (m *Manager) GetAll() []*IMemTable {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]*IMemTable, len(m.IMems))
	copy(out, m.IMems)
	return out
}

// promoteLocked：仅在已持有写锁的情况下调用！
func (m *Manager) promoteLocked() *IMemTable {
	var evicted *IMemTable
	if len(m.IMems) >= maxIMemTableCount {
		evicted = m.IMems[0]
		m.IMems = m.IMems[1:]
	}

	imem := NewIMemTable(m.Mem)
	m.IMems = append(m.IMems, imem)
	m.Mem = NewMemTable(util.IDGen.Next(), config.Conf.WALPath)

	return evicted
}

func (m *Manager) CanInsert(pair kv.KeyValuePair) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.Mem.CanInsert(pair)
}

// Recover 从 WALManager 恢复所有 memtable 数据，最多构造 10 个 IMemTable 和 1 个 MemTable
func (m *Manager) Recover() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 收集所有 WAL 恢复数据
	files, err := os.ReadDir(config.GetWALPath()) // 返回的是文件名，而不是文件完整路径
	if err != nil {
		log.Errorf("failed to read WAL directory %s: %s", config.GetWALPath(), err.Error())
		return fmt.Errorf("failed to read WAL directory %s: %w", config.GetWALPath(), err)
	}
	// 将所有 WAL 按照 ID 排序，最新的加载为 memtable，其余加载为 imemtable
	sort.Slice(files, func(i, j int) bool { return util.ExtractID(files[i].Name()) < util.ExtractID(files[j].Name()) })
	// 构建 IMemTable 和 MemTable
	for i, file := range files {
		mem := NewMemTableWithoutWAL()
		if err = mem.RecoverFromWAL(file.Name()); err != nil {
			log.Errorf("recover from WAL %s failed: %s", file.Name(), err.Error())
			return fmt.Errorf("recover from WAL %s failed: %w", file.Name(), err)
		}
		if i == len(files)-1 {
			m.Mem = mem
			// 并且处理自增 id 的逻辑
			idGenerator.Add(util.ExtractID(file.Name()))
		} else {
			m.IMems = append(m.IMems, NewIMemTable(mem))
		}
	}

	// 保证最多 10 个 IMemTable
	if len(m.IMems) > maxIMemTableCount {
		m.IMems = m.IMems[len(m.IMems)-maxIMemTableCount:]
	}

	return nil
}
