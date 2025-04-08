# 内存驻留组件

## 设计

为了追求读取性能优化，采用在内存中存储一张 Memtable 和十张 Immutable Memtable 操作，方便查询时快速找到 key。
Immutable Memtable 采用 FIFO 的淘汰机制，被淘汰的 Immutable Memtable 会被持久化到磁盘中，形成一个 SSTable 文件。

为什么不用 LRU 机制管理 Immutable Memtable？

因为 LSM 树在查询同一个 key 时，需要匹配最新的 key，因此需要查询最新的 Immutable Memtable。

## Memtable

内存驻留组件由**Memtable**和**Immutable Memtable**组成。数据在**Memtable**中通常以有序的**跳表**结构进行存储，以保证磁盘数据的有序行。
**Memtable**负责缓冲数据记录，**Immutable Memtable**完成对于数据的落盘操作。

内存驻留组件用于临时存储新写入的数据，以进一步提高写入性能。一旦其达到一定大小，它将被写入到磁盘或闪存，并形成一个**SSTable文件（Sorted String Table）**，其中数据按键有序排列。

**WAL（Write-Ahead Log，预写日志）机制**是现代数据库和存储引擎中确保数据持久性与一致性的重要手段。它的核心思想是：

> **在对内存数据结构（如 MemTable）进行变更前，先将变更操作记录到持久化日志中（WAL），确保即使系统崩溃，也可以通过日志恢复数据。**

---

## WAL 机制

当用户执行写操作（如插入或更新 key-value 数据）时，系统不会立即将数据写入磁盘上的最终结构（如 SSTable），而是：

1. **首先将操作追加写入 WAL 文件**（顺序写，磁盘友好）
2. **然后更新内存中的 MemTable**
3. 当 MemTable 达到一定大小，**会被转为不可变的 Immutable MemTable**
4. 后台线程异步将 Immutable MemTable 持久化为 SSTable
5. 对应的 WAL 也会被删除或归档

---

### WAL 与 MemTable 的配合关系

| 角色           | 功能                    |
|--------------|-----------------------|
| **WAL**      | 提供崩溃恢复保证；先写入磁盘，防止数据丢失 |
| **MemTable** | 内存中的有序结构，提供高效的读写访问    |

它们之间的工作流程如下：

```
用户写入 → 写 WAL（持久化） → 更新 MemTable（内存）
```

**这样设计的好处是**：

- 即使系统崩溃，只要 WAL 存在，就能恢复 MemTable 的内容，不会丢失已提交的数据；
- MemTable 提供高速访问，而 WAL 提供持久保证；
- 转换为 SSTable 后，WAL 可被安全删除，节省空间。

---

### 一个典型的写入流程

1. 用户写入数据
2. 写入 WAL：`append("put foo=bar")`
3. 更新 MemTable：`MemTable.Put("foo", "bar")`
4. 如果 MemTable 满了，转为 Immutable，并生成新的 WAL
5. 后台将 Immutable MemTable 持久化为 SSTable
6. 成功后删除对应 WAL 文件

![Alt text](../docs/pics/1628835101487.png)

---

### WAL 的优势与设计目的

- **数据可靠性保障**：只要写入 WAL 成功，数据就不会丢。
- **崩溃恢复能力**：系统重启时重新 replay WAL，即可恢复 MemTable。
- **顺序写优化性能**：WAL 是顺序写，效率高，适合 SSD/HDD。
- **配合 LSM Tree 实现高吞吐写入性能**
