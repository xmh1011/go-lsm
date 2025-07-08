package sstable

import (
	"fmt"

	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
)

// Compaction 执行 Level0 的同步合并，并触发 Level1 及以上的异步合并。
// 合并流程：
// 1. 收集 Level0 文件，解码其 DataBlock，并统计全局 key 区间。
// 2. 从 Level1 中找出与该区间交集的文件，将其 DataBlock 一并取出。
// 3. 使用归并排序将所有块合并分块，产出新 SSTable（写入 Level1）。
// 4. 删除旧 Level0 和 Level1 文件，并加入新文件记录。
// 5. 如果 Level1 超限，异步触发后续合并。
// Compaction 执行 Level0 的同步合并，并触发后续异步合并
func (m *Manager) Compaction() error {
	// 等待同一层级的压缩完成
	if err := m.waitCompaction(minSSTableLevel); err != nil {
		log.Errorf("wait compaction for level %d error: %s", minSSTableLevel, err.Error())
		return fmt.Errorf("wait compaction error: %w", err)
	}

	// 检查 Level0 是否需要压缩
	if !m.isLevelNeedToBeMerged(minSSTableLevel) {
		log.Debug("level 0 not need to be merged")
		return nil
	}

	// 开始 Level0 压缩
	if err := m.compactLevel(minSSTableLevel); err != nil {
		log.Errorf("compact level %d error: %s", minSSTableLevel, err.Error())
		return fmt.Errorf("compact level %d error: %w", minSSTableLevel, err)
	}

	// 触发下一层级异步压缩（如果需要）
	if m.isLevelNeedToBeMerged(minSSTableLevel + 1) {
		go m.asyncCompactLevel(minSSTableLevel + 1)
	}

	return nil
}

// asyncCompactLevel 异步合并指定层级（Level1 及以上）
func (m *Manager) asyncCompactLevel(level int) {
	for {
		// 等待同一层级的压缩完成
		if err := m.waitCompaction(level); err != nil {
			log.Errorf("wait compaction error: %v", err)
			return
		}

		// 检查是否需要压缩
		if !m.isLevelNeedToBeMerged(level) {
			return
		}

		// 执行压缩
		if err := m.compactLevel(level); err != nil {
			log.Errorf("async compaction at level %d error: %v", level, err)
			return
		}

		// 如果下一层级仍需压缩，继续循环（仅对中间层级）
		if level < maxSSTableLevel && m.isLevelNeedToBeMerged(level+1) {
			continue
		}
		return
	}
}

// compactLevel 同步合并指定层级
func (m *Manager) compactLevel(level int) error {
	// 标记当前层级开始压缩
	m.startCompaction(level)
	defer m.endCompaction(level)

	// 1. 读取当前层级的所有键值对
	files := m.getFilesByLevel(level)
	// 对于 level 1 及以上的层级
	// 按照时间顺序，只合并超出数量的旧文件
	if level > minSSTableLevel {
		files = files[:maxFileNumsInLevel(level)]
	}
	allPairs, err := m.loadLevelData(files)
	if err != nil {
		log.Errorf("load level %d data error: %s", level, err.Error())
		return fmt.Errorf("load level %d data error: %w", level, err)
	}

	// 2. 加载重叠文件
	var nextLevelPairs []kv.KeyValuePair
	var oldNextFiles []string
	if level < maxSSTableLevel {
		minK, maxK := getGlobalKeyRangeFromPairs(allPairs)
		nextLevelPairs, oldNextFiles, err = m.mergeNextLevelFiles(level+1, minK, maxK)
		if err != nil {
			log.Errorf("merge next level files error: %s", err.Error())
			return fmt.Errorf("merge next level files error: %w", err)
		}
		allPairs = append(allPairs, nextLevelPairs...)
	}

	// 3. 合并并生成新 SSTable
	newTables := CompactAndMergeKVs(allPairs, level+1) // 目标层级为当前+1

	// 4. 清理旧文件
	if err := m.removeOldSSTables(files, level); err != nil {
		log.Errorf("remove old SSTables error: %s", err.Error())
		return fmt.Errorf("remove old SSTables error: %w", err)
	}
	if len(oldNextFiles) > 0 {
		if err := m.removeOldSSTables(oldNextFiles, level+1); err != nil {
			log.Errorf("remove old next level SSTables error: %s", err.Error())
			return fmt.Errorf("remove old next level SSTables error: %w", err)
		}
	}

	// 5. 添加新文件
	if err := m.addNewSSTables(newTables); err != nil {
		log.Errorf("add new SSTables error: %s", err.Error())
		return fmt.Errorf("add new SSTables error: %w", err)
	}

	// 6. 如果目标层级仍需压缩，递归处理（仅对中间层级）
	if level < maxSSTableLevel && m.isLevelNeedToBeMerged(level+1) {
		return m.compactLevel(level + 1)
	}

	return nil
}

// waitCompaction 等待指定层级的压缩完成
func (m *Manager) waitCompaction(level int) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for m.isLevelCompacting(level) {
		log.Debugf("level %d is compacting, waiting...", level)
		m.compactionCond.Wait()
	}
	return nil
}

// startCompaction 标记层级开始压缩
func (m *Manager) startCompaction(level int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.compactingLevels[level] = true
}

// endCompaction 标记层级压缩完成并广播通知
func (m *Manager) endCompaction(level int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.compactingLevels, level)
	m.compactionCond.Broadcast()
}

// isLevelCompacting 检查层级是否正在压缩
func (m *Manager) isLevelCompacting(level int) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.compactingLevels[level]
}

// loadLevelData 加载指定层级的所有键值对
func (m *Manager) loadLevelData(files []string) ([]kv.KeyValuePair, error) {
	allPairs := make([]kv.KeyValuePair, 0)

	for _, path := range files {
		sst, ok := m.getSSTableByPath(path)
		if !ok {
			log.Errorf("sstable not found for path: %s", path)
			continue
		}

		pairs, err := sst.GetDataBlockFromFile(path)
		if err != nil {
			log.Errorf("decode sstable from file %s error: %s", path, err.Error())
			return nil, fmt.Errorf("decode sstable from file %s error: %w", path, err)
		}

		allPairs = append(allPairs, pairs...)
	}

	return allPairs, nil
}

// mergeNextLevelFiles 合并下一层级的重叠文件
func (m *Manager) mergeNextLevelFiles(level int, minK, maxK kv.Key) ([]kv.KeyValuePair, []string, error) {
	nextLevelFiles := m.getFilesByLevel(level)
	oldFiles := make([]string, 0)
	allPairs := make([]kv.KeyValuePair, 0)

	for _, path := range nextLevelFiles {
		sst, ok := m.getSSTableByPath(path)
		if !ok {
			log.Errorf("sstable not found for path: %s", path)
			continue
		}

		if overlapRange(minK, maxK, sst) {
			pairs, err := sst.GetDataBlockFromFile(path)
			if err != nil {
				log.Errorf("load data blocks error: %v", err)
				return nil, nil, err
			}
			allPairs = append(allPairs, pairs...)
			oldFiles = append(oldFiles, path)
		}
	}

	return allPairs, oldFiles, nil
}

// getGlobalKeyRangeFromPairs 从键值对中计算全局 Key 范围
func getGlobalKeyRangeFromPairs(pairs []kv.KeyValuePair) (kv.Key, kv.Key) {
	if len(pairs) == 0 {
		return "", ""
	}

	minKey, maxKey := pairs[0].Key, pairs[0].Key
	for _, pair := range pairs {
		if pair.Key < minKey {
			minKey = pair.Key
		}
		if pair.Key > maxKey {
			maxKey = pair.Key
		}
	}
	return minKey, maxKey
}

// overlapRange 判断 global range [minKey, maxKey] 是否与 sst 索引区间有交集
func overlapRange(minKey, maxKey kv.Key, sst *SSTable) bool {
	return !(sst.Header.MinKey < minKey || sst.Header.MaxKey > maxKey)
}
