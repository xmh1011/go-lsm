package sstable

import (
	"fmt"
	"os"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
)

// SearchFromFile searches for the specified key in the SSTable file with the given ID.
// 无需加载整个文件，提升性能
// 如果不包含 key， 返回nil
func SearchFromFile(path string, key kv.Key) ([]byte, error) {
	var err error
	table := NewSSTable()
	table.file, err = os.Open(path)
	if err != nil {
		log.Errorf("open file %s error: %s", path, err.Error())
		return nil, fmt.Errorf("open file error: %w", err)
	}
	defer table.Close()

	// 1. 先读取footer
	err = table.Footer.DecodeFrom(table.file)
	if err != nil {
		log.Errorf("decode footer error: %s", err.Error())
		return nil, fmt.Errorf("decode footer error: %w", err)
	}
	// 2. 根据footer中的meta index block handler记录的布隆过滤器信息，读取布隆过滤器记录的二进制数据
	err = table.FilterBlock.DecodeFrom(table.file, table.Footer.MetaIndexHandle)
	if err != nil {
		log.Errorf("decode meta index block error: %s", err.Error())
		return nil, fmt.Errorf("decode meta index block error: %w", err)
	}
	// 3. 根据布隆过滤器的记录的二进制数据，查询key是否存在，如果不存在则直接返回
	if ok := table.FilterBlock.MayContain(key); !ok {
		log.Debugf("key %s not found in file %s", key, path)
		return nil, nil
	}
	// 4. 根据footer中的index block handler记录的 index block的起始位置和大小,读取index block
	err = table.IndexBlock.DecodeFrom(table.file, table.Footer.IndexHandle)
	if err != nil {
		log.Errorf("decode index block error: %s", err.Error())
		return nil, fmt.Errorf("decode index block error: %w", err)
	}
	// 5. 通过index block查询key所在的data block
	it := NewSSTableIterator(table)
	it.Seek(key)
	if key != it.Key() {
		log.Debugf("required key: %s, found key [%s] in %s", key, it.Key(), path)
		return nil, fmt.Errorf("required key: %s, found key [%s] in %s", key, it.Key(), path)
	}
	// 6. 返回key对应的value
	value := it.Value()
	if value == nil {
		log.Errorf("key found in %s, but value is nil", path)
		return nil, fmt.Errorf("key found in %s, but value is nil", path)
	}

	return value, nil
}
