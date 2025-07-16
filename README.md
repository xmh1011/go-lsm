# go-lsm

`go-lsm` is a simple LSM tree implementation in Go.

## References

- [mini-lsm](https://skyzh.github.io/mini-lsm/)
- [go-lsm](https://github.com/SarthakMakhija/go-lsm)
- [reading-source-code-of-leveldb-1.23](https://github.com/SmartKeyerror/reading-source-code-of-leveldb-1.23)
- [leveldb](https://github.com/merlin82/leveldb)
- 美 彼得罗夫 Petrov, Alex.数据库系统内幕[M].机械工业出版社,2020.

## Design

In the implementation of LSM-Tree (Log-Structured Merge-Tree), a common design pattern is to utilize SkipList-based MemTable (In-Memory Table) and SSTable (Sorted String Table). This architecture is widely adopted in popular LSM-Tree-based databases like LevelDB and RocksDB.

![./docs/pics/leveldb.png](./docs/pics/leveldb.png)

You can refer to [description](./docs/description.md)

## Log

- Day 1
  - KV structure
  - SkipList
- Day 2
  - MemTable and IMemTable
  - WAL
- Day 3 - Day 6
  - SSTable
  - MemTable Builder
  - SSTable Manager
- Day 7
  - Compaction
  - Test

![./docs/pics/log.jpg](./docs/pics/log.jpg)

## Benchmark

```bash
make bench
```

```text
goos: darwin
goarch: arm64
pkg: github.com/xmh1011/go-lsm/database
cpu: Apple M3
BenchmarkPut-8           2943309             30952 ns/op
BenchmarkGet-8           171483172           206.1 ns/op
BenchmarkDelete-8        1817216             21721 ns/op
PASS
ok      github.com/xmh1011/go-lsm/database      227.578s
```

```bash
make benchmark
```

```text
==============================================
 测试目录   : /Users/xiaominghao/code/go-lsm/data
 循环轮数   : 5
 写入总数   : 5000000
 写入耗时   : 53.780590733s (平均)
 写 ops/s  : 18594.07 (平均)
 写 ns/op  : 53780.59 (平均)
 读取总数   : 5000
 读取耗时   : 49.22855ms (平均)
 读 ops/s  : 20313.42 (平均)
 读 ns/op  : 49228.55 (平均)
==============================================
```

## Data Directory

```text
➜  go-lsm git:(dev) ✗ tree -h -L 3 data
[ 128]  data
├── [ 288]  sstable
│   ├── [ 128]  0-level
│   │   ├── [2.2M]  866.sst
│   │   └── [2.2M]  871.sst
│   ├── [ 320]  1-level
│   │   ├── [2.2M]  814.sst
│   │   ├── [2.2M]  858.sst
│   │   ├── [2.2M]  859.sst
│   │   ├── [2.2M]  860.sst
│   │   ├── [2.2M]  861.sst
│   │   ├── [2.2M]  862.sst
│   │   ├── [2.2M]  863.sst
│   │   └── [2.2M]  864.sst
│   ├── [ 576]  2-level
│   │   ├── [2.2M]  916.sst
│   │   ├── [2.2M]  917.sst
│   │   ├── [2.2M]  918.sst
│   │   ├── [2.2M]  919.sst
│   │   ├── [2.2M]  920.sst
│   │   ├── [2.2M]  921.sst
│   │   ├── [2.2M]  922.sst
│   │   ├── [2.2M]  923.sst
│   │   ├── [2.2M]  924.sst
│   │   ├── [2.2M]  925.sst
│   │   ├── [2.2M]  926.sst
│   │   ├── [2.2M]  927.sst
│   │   ├── [2.2M]  928.sst
│   │   ├── [2.2M]  929.sst
│   │   ├── [2.2M]  930.sst
│   │   └── [2.2M]  931.sst
│   ├── [1.1K]  3-level
│   │   ├── [2.2M]  961.sst
│   │   ├── [2.2M]  962.sst
│   │   ├── [2.2M]  963.sst
│   │   ├── [2.2M]  964.sst
│   │   ├── [2.2M]  965.sst
│   │   ├── [2.2M]  966.sst
│   │   ├── [2.2M]  967.sst
│   │   ├── [2.2M]  968.sst
│   │   ├── [2.2M]  969.sst
│   │   ├── [2.2M]  970.sst
│   │   ├── [2.2M]  971.sst
│   │   ├── [2.2M]  972.sst
│   │   ├── [2.2M]  973.sst
│   │   ├── [2.2M]  974.sst
│   │   ├── [2.2M]  975.sst
│   │   ├── [2.2M]  976.sst
│   │   ├── [2.2M]  977.sst
│   │   ├── [2.2M]  978.sst
│   │   ├── [2.2M]  979.sst
│   │   ├── [2.2M]  980.sst
│   │   ├── [2.2M]  981.sst
│   │   ├── [2.2M]  982.sst
│   │   ├── [2.2M]  983.sst
│   │   ├── [2.2M]  984.sst
│   │   ├── [2.2M]  985.sst
│   │   ├── [2.2M]  986.sst
│   │   ├── [2.2M]  987.sst
│   │   ├── [2.2M]  988.sst
│   │   ├── [2.2M]  989.sst
│   │   ├── [2.2M]  990.sst
│   │   ├── [2.2M]  991.sst
│   │   └── [2.2M]  992.sst
│   ├── [1.3K]  4-level
│   │   ├── [2.2M]  1007.sst
│   │   ├── [2.2M]  1008.sst
│   │   ├── [2.2M]  1009.sst
│   │   ├── [2.2M]  1010.sst
│   │   ├── [2.2M]  1011.sst
│   │   ├── [2.2M]  1012.sst
│   │   ├── [2.2M]  1013.sst
│   │   ├── [2.2M]  1014.sst
│   │   ├── [2.2M]  1015.sst
│   │   ├── [2.2M]  1016.sst
│   │   ├── [2.2M]  1017.sst
│   │   ├── [2.2M]  1018.sst
│   │   ├── [2.2M]  1019.sst
│   │   ├── [2.2M]  1020.sst
│   │   ├── [2.2M]  1021.sst
│   │   ├── [2.2M]  1022.sst
│   │   ├── [2.2M]  1023.sst
│   │   ├── [2.2M]  1024.sst
│   │   ├── [2.2M]  1025.sst
│   │   ├── [2.2M]  1026.sst
│   │   ├── [2.2M]  1027.sst
│   │   ├── [2.2M]  1028.sst
│   │   ├── [2.2M]  1029.sst
│   │   ├── [2.2M]  1030.sst
│   │   ├── [2.2M]  1031.sst
│   │   ├── [2.2M]  1032.sst
│   │   ├── [2.2M]  1033.sst
│   │   ├── [2.2M]  1034.sst
│   │   ├── [2.2M]  1035.sst
│   │   ├── [196K]  1036.sst
│   │   ├── [2.2M]  876.sst
│   │   ├── [2.2M]  877.sst
│   │   ├── [2.2M]  878.sst
│   │   ├── [2.2M]  879.sst
│   │   ├── [2.2M]  880.sst
│   │   ├── [2.2M]  881.sst
│   │   ├── [2.2M]  882.sst
│   │   ├── [2.2M]  883.sst
│   │   ├── [2.2M]  884.sst
│   │   └── [2.2M]  885.sst
│   ├── [  64]  5-level
│   └── [  64]  6-level
└── [ 416]  wal
    ├── [1.7M]  109.wal
    ├── [1.7M]  110.wal
    ├── [1.7M]  111.wal
    ├── [1.7M]  112.wal
    ├── [1.7M]  113.wal
    ├── [1.7M]  114.wal
    ├── [1.7M]  115.wal
    ├── [1.7M]  116.wal
    ├── [1.7M]  117.wal
    ├── [1.7M]  118.wal
    └── [1.5M]  119.wal

10 directories, 120 files
```

## TODO List

- Performance Optimization
- Tombstone
- Version Control
- MVCC
- Snapshot
- 2 PL
- Data Block Compression
