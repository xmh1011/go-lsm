// 若Level0层中的文件数量超出限制，则开始进行合并操作。对于Level0层的合并操作来说，
// 需要将所有的Level0层中的 SSTable 与 Level1 层的中部分SSTable 进行合并，随后将产生的新 SSTable 文件写入到Level1 层中。
// 1.先统计Level0 层中所有 SSTable 所覆盖的键的区间。然后在 Level 1层中找到与此区间有交集的所有 SSTable 文件。
// 2.使用归并排序，将上述所有涉及到的 SSTable 进行合并，并将结果每2MB 分成一个新的 SSTable 文件(最后一个 SSTable 可以不足2MB)，写入到 Level 1 中
// 3.若产生的文件数超出 Level1 层限定的数目，则从Level1的 SSTable中，优先选择时间戳最小的若干个文件(时间戳相等选择键最小的文件)，
// 使得文件数满足层数要求，以同样的方法继续向下一层合并(若没有下一层，则新建一层)。

package sstable

import (
	"github.com/xmh1011/go-lsm/kv"
	"github.com/xmh1011/go-lsm/log"
	"github.com/xmh1011/go-lsm/sstable/block"
)

// Compaction 执行 Level0 的同步合并，并触发 Level1 及以上的异步合并。
// 合并流程：
// 1. 收集 Level0 文件，解码其 DataBlocks，并统计全局 key 区间。
// 2. 从 Level1 中找出与该区间交集的文件，将其 DataBlocks 一并取出。
// 3. 使用归并排序将所有块合并分块，产出新 SSTable（写入 Level1）。
// 4. 删除旧 Level0 和 Level1 文件，并加入新文件记录。
// 5. 如果 Level1 超限，异步触发后续合并。
func (m *Manager) Compaction() error {
	if !m.isLevelNeedToBeMerged(minSSTableLevel) {
		log.Debug("level 0 not need to be merged")
		return nil
	}

	level0Files := m.getFilesByLevel(minSSTableLevel)
	if len(level0Files) == 0 {
		log.Debug("level 0 files not exist")
		return nil
	}

	var allBlocks []*block.DataBlock
	var oldLevel0Files []string
	for _, path := range level0Files {
		sst := NewSSTable()
		if err := sst.DecodeFrom(path); err != nil {
			log.Errorf("decode L0 sstable from file %s error: %s", path, err.Error())
			return err
		}
		allBlocks = append(allBlocks, sst.DataBlocks...)
		oldLevel0Files = append(oldLevel0Files, path)
	}

	minK, maxK := getGlobalKeyRange(allBlocks)
	newBlocks, oldLevel1Files, err := m.mergeNextLevelFiles(minSSTableLevel+1, minK, maxK)
	if err != nil {
		log.Errorf("merge next level files error: %s", err.Error())
		return err
	}
	allBlocks = append(allBlocks, newBlocks...) // 将 Level1 的 blocks 加入到 allBlocks 中

	newTables := CompactAndMergeBlocks(allBlocks, minSSTableLevel+1)
	if err := m.removeOldSSTables(oldLevel0Files, minSSTableLevel); err != nil {
		return err
	}
	if err := m.removeOldSSTables(oldLevel1Files, minSSTableLevel+1); err != nil {
		return err
	}
	if err := m.addNewSSTables(newTables, minSSTableLevel+1); err != nil {
		return err
	}

	if m.isLevelNeedToBeMerged(minSSTableLevel+1) && !m.isCompacting() {
		m.startCompaction(minSSTableLevel + 1)
		go m.asyncCompactLevel(minSSTableLevel + 1)
	}

	return nil
}

// asyncCompactLevel 异步合并指定层（Level1 及以上），使用条件变量等待合并完成。
func (m *Manager) asyncCompactLevel(level int) {
	for m.isLevelNeedToBeMerged(level) {
		if err := m.compactLevel(level); err != nil {
			log.Errorf("async compaction at level %d error: %s", level, err.Error())
			break
		}
	}

	m.mu.Lock()
	m.compacting = false
	m.compactingLevel = -1
	m.compactionCond.Broadcast()
	m.mu.Unlock()
}

// compactLevel 同步合并指定层（Level1 及以上）。
func (m *Manager) compactLevel(level int) error {
	if level >= maxSSTableLevel {
		return m.newLevelCompaction(level)
	}
	if !m.isLevelNeedToBeMerged(level) {
		return nil
	}

	// 1. 收集当前层文件，要按时间戳排序
	var allBlocks []*block.DataBlock
	var oldFilesLevel []string
	curLevelFiles := m.getSortedFilesByLevel(level)
	if len(curLevelFiles) == 0 {
		return nil
	}

	// 将当前层最老的文件加载到内存，与下一层所有文件合并
	expectedFileNum := maxFileNumsInLevel(level)
	for i := 0; i < len(curLevelFiles)-expectedFileNum; i++ {
		path := curLevelFiles[i]
		sst := NewSSTable()
		if err := sst.DecodeFrom(path); err != nil {
			log.Errorf("decode sstable from file %s error: %s", path, err.Error())
			return err
		}
		allBlocks = append(allBlocks, sst.DataBlocks...)
		oldFilesLevel = append(oldFilesLevel, path)
	}

	// 2. 找到下一层
	nextLevel := level + 1
	minK, maxK := getGlobalKeyRange(allBlocks)
	newBlocks, oldNextFiles, err := m.mergeNextLevelFiles(nextLevel, minK, maxK)
	if err != nil {
		log.Errorf("merge next level files error: %s", err.Error())
		return err
	}
	allBlocks = append(allBlocks, newBlocks...)

	// 合并
	newTables := CompactAndMergeBlocks(allBlocks, nextLevel)
	// 移除旧文件
	if err := m.removeOldSSTables(oldFilesLevel, level); err != nil {
		return err
	}
	if err := m.removeOldSSTables(oldNextFiles, nextLevel); err != nil {
		return err
	}
	// 加新
	if err := m.addNewSSTables(newTables, nextLevel); err != nil {
		return err
	}
	// 若 nextLevel 又超限 => 递归
	if m.isLevelNeedToBeMerged(nextLevel) {
		return m.compactLevel(nextLevel)
	}

	return nil
}

// newLevelCompaction 在 maxSSTableLevel 之后再新建一层
func (m *Manager) newLevelCompaction(level int) error {
	newLevel := level + 1
	curFiles := m.getFilesByLevel(level)
	if len(curFiles) == 0 {
		return nil
	}

	var allBlocks []*block.DataBlock
	for _, f := range curFiles {
		sst := NewSSTable()
		if err := sst.DecodeFrom(f); err != nil {
			log.Errorf("decode sstable from file %s error: %s", f, err.Error())
			return err
		}
		allBlocks = append(allBlocks, sst.DataBlocks...)
	}

	newTables := CompactAndMergeBlocks(allBlocks, newLevel)
	if err := m.removeOldSSTables(curFiles, level); err != nil {
		return err
	}
	if err := m.addNewSSTables(newTables, newLevel); err != nil {
		return err
	}

	return nil
}

func (m *Manager) mergeNextLevelFiles(level int, minK, maxK kv.Key) ([]*block.DataBlock, []string, error) {
	nextLevelFiles := m.getFilesByLevel(level)
	var oldLevelFiles []string
	var newBlocks []*block.DataBlock
	for _, path := range nextLevelFiles {
		// 首先判断在内存中有没有
		var sst *SSTable
		sst, ok := m.isFileInMemoryAndReturn(path)
		if !ok {
			sst = NewSSTable()
			if err := sst.DecodeMetaData(path); err != nil {
				log.Errorf("load meta block to memory error: %s", err.Error())
				return nil, nil, err
			}
		}
		// 判断 overlap
		if overlapRange(minK, maxK, sst) {
			// 加载 data
			if err := sst.DecodeDataBlocks(); err != nil {
				log.Errorf("load data blocks to memory error: %s", err.Error())
				return nil, nil, err
			}
			newBlocks = append(newBlocks, sst.DataBlocks...)
			oldLevelFiles = append(oldLevelFiles, path)
		}
		if !ok {
			sst.Close() // 如果没有在内存中 => 需要关闭文件
		}
	}

	return newBlocks, oldLevelFiles, nil
}

// getGlobalKeyRange 返回 blocks 中的最小、最大 key
func getGlobalKeyRange(blocks []*block.DataBlock) (kv.Key, kv.Key) {
	if len(blocks) == 0 {
		return "", ""
	}
	minKey := blocks[0].Records[0].Key
	maxKey := blocks[0].Records[len(blocks[0].Records)-1].Key
	for _, blk := range blocks {
		if len(blk.Records) == 0 {
			continue
		}
		s := blk.Records[0].Key
		e := blk.Records[len(blk.Records)-1].Key
		if s < minKey {
			minKey = s
		}
		if e > maxKey {
			maxKey = e
		}
	}
	return minKey, maxKey
}

func (m *Manager) isCompacting() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.compacting
}

func (m *Manager) startCompaction(level int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.compacting = true
	m.compactingLevel = level
}

// overlapRange 判断 global range [minKey, maxKey] 是否与 sst 索引区间有交集
func overlapRange(minKey, maxKey kv.Key, sst *SSTable) bool {
	if len(sst.IndexBlock.Indexes) == 0 {
		return false
	}
	sMin := sst.IndexBlock.StartKey
	sMax := sst.IndexBlock.Indexes[len(sst.IndexBlock.Indexes)-1].SeparatorKey

	// 若 sMax < minKey 或 sMin > maxKey => 无交集
	return !(sMax < minKey || sMin > maxKey)
}
