package database

import (
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/memtable"
	"github.com/xmh1011/go-lsm/sstable"
)

type Database struct {
	name      string
	MemTables *memtable.Manager
	SSTables  *sstable.Manager
}

func Open(name string) *Database {
	return &Database{
		name:      name,
		MemTables: memtable.NewMemtableManager(),
		SSTables:  sstable.NewSSTableManager(),
	}
}

func (d *Database) Get(key string) ([]byte, error) {
	value := d.MemTables.Search(kv.Key(key))
	if value != nil {
		return value, nil
	}

	value, err := d.SSTables.Search(kv.Key(key))
	if err != nil {
		log.Errorf("search key %s in sstable error: %s", key, err.Error())
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	return value, nil
}

func (d *Database) Put(key string, value []byte) error {
	imem, err := d.MemTables.Insert(kv.KeyValuePair{Key: kv.Key(key), Value: value})
	if err != nil {
		log.Errorf("insert key %s error: %s", key, err.Error())
		return err
	}
	d.createNewSSTable(imem)
	return nil
}

func (d *Database) Delete(key kv.Key) error {
	imem, err := d.MemTables.Delete(key)
	if err != nil {
		return err
	}
	d.createNewSSTable(imem)
	return nil
}

func (d *Database) Recover() error {
	// 1. 恢复内存中的 Memtable
	if err := d.MemTables.Recover(); err != nil {
		log.Errorf("recover memtable error: %s", err.Error())
		return err
	}

	// 2. 恢复磁盘中的 SSTable
	if err := d.SSTables.Recover(); err != nil {
		log.Errorf("recover sstable error: %s", err.Error())
		return err
	}

	return nil
}

func (d *Database) createNewSSTable(imem *memtable.IMemtable) {
	if imem == nil {
		return
	}
	err := d.SSTables.CreateNewSSTable(imem)
	if err != nil {
		log.Errorf("create new sstable error: %s", err.Error())
		return
	}
}
