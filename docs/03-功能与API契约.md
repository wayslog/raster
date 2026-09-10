# C++ 实现的功能与 API 契约

本文面向将 `FASTER/cc` 改写为 Rust 的前期调查。依据是当前工作区源代码的静态阅读；未编译、未运行测试、未验证崩溃恢复或性能。文中 API 标识符保留原文，解释使用中文；行号对应本次调查快照。这里的“公共 API”指 C++ 模板与头文件接口，不是 HTTP API、RPC 或稳定 C ABI。

## 1. 能力清单与实现边界

| 能力 | 当前 C++ 入口 / 行为 | 主要证据 |
|---|---|---|
| 通用键值存储 | `FasterKv<K,V,D,H,OH>`；键、值、设备、索引均可替换 | [faster.h:93](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L93) |
| 单键读写 | `Read`、`Upsert`、`Rmw`、`Delete`；直接返回状态，读取结果放入调用者上下文 | [faster.h:209](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L209) |
| 内存与磁盘混合日志 | 可变区就地更新、不可变/磁盘记录追加或异步读取；可使用无后备存储配置 | [faster.h:139](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L139) |
| 异步操作推进 | `CompletePending`、`Refresh`；异步上下文深拷贝和完成回调 | [faster.h:948](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L948)、[async.h:27](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/async.h#L27) |
| 线程会话与持续恢复 | 会话 GUID、操作序列号、`ContinueSession` 返回已持久化序列号 | [faster.h:696](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L696) |
| 检查点与恢复 | 完整、仅索引、仅日志三类检查点；按 token 恢复 | [faster.h:229](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L229) |
| 日志压缩与回收 | 旧 `Compact`、查索引式 `CompactWithLookup`、自动压缩、`ShiftBeginAddress` | [faster.h:239](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L239) |
| 在线索引扩容 | `GrowIndex` 启动协作阶段，通过回调返回新大小 | [faster.h:3545](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L3545) |
| 读缓存 | 构造配置开启；默认关闭 | [config.h:30](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/config.h#L30) |
| 热冷分层存储 | `F2Kv` 组合热 `FasterKv` 与冷 `FasterKv`，默认热内存索引、冷 `ColdIndex` | [f2.h:19](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L19) |
| 变长键值 | 上下文提供大小与深键写入；记录布局与对齐由模板约束 | [internal_contexts_f2.h:19](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/internal_contexts_f2.h#L19)、[test_types.h:162](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/test/test_types.h#L162) |
| 日志记录扫描 | `LogRecordIterator`、页迭代器；按物理地址扫描，不等于当前有效键集合枚举 | [log_scan.h:764](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/log_scan.h#L764) |
| 配置与诊断 | TOML 构造、索引分布、日志跨度、会话数量；条件编译统计 | [faster.h:254](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L254) |
| 本地 / 云设备 | 本地文件系统、空设备、可选 Azure Blob 分层设备 | [file_system_disk.h:23](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/device/file_system_disk.h#L23)、[storage.h:48](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/device/storage.h#L48) |

在已审查的 `FasterKv` / `F2Kv` 公共入口中，没有 SQL、范围有序索引、多键事务、网络服务、复制协议或 Rust 风格 `Future` 接口；不要将仓库其他语言版本的能力直接算入 C++ 迁移范围。独立追加日志产品也不应仅凭 `hlog` 内部成员就认定已经有同等高层 C++ API。

## 2. FasterKv 构造和公共数据

源签名：

```cpp
template<class K, class V, class D, class H = MemHashIndex<D>, class OH = H>
class FasterKv;

FasterKv(IndexConfig index_config, uint64_t hlog_mem_size,
         const std::string& filepath,
         double hlog_mutable_fraction = DEFAULT_HLOG_MUTABLE_FRACTION,
         ReadCacheConfig rc_config = DEFAULT_READ_CACHE_CONFIG,
         HlogCompactionConfig hlog_compaction_config = DEFAULT_HLOG_COMPACTION_CONFIG,
         bool pre_allocate_log = false, const std::string& config = "");
FasterKv(const Config& config);
```

`H` 默认 `MemHashIndex<D>`；`OH` 用于配合另一个存储实例，主要服务于 F2。参数分别控制索引、日志内存预算、存储路径、可变区比例、缓存、自动压缩、预分配以及设备专用配置字符串。`filepath.empty()` 被传为 `hasNoBackingStorage`；不能把空路径构造当作持久化保证。禁止拷贝构造。析构会等待系统恢复 REST 阶段并停止自动压缩线程，不能替代正确的会话和持久化生命周期。证据：[faster.h:93](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L93)、[faster.h:139](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L139)、[faster.h:179](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L179)。

`Config` 包含 `index_config`、`hlog_config`、`hlog_compaction_config`、`rc_config`、`filepath`。单层日志默认可变比例为 `0.9`，读缓存和自动压缩默认关闭。F2 使用自己的默认值，不能共用单层默认策略。证据：[config.h:21](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/config.h#L21)、[config.h:207](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/config.h#L207)。

原实现公开了 `disk`、`hlog`、`hash_index_`、`read_cache_`，并公开部分名为 `Internal*` 的方法与 `SetRefreshCallback`。这意味着 C++ 的 `public` 不完全等于应承诺兼容的用户接口；Rust 设计需区分稳定操作入口与引擎扩展接口。证据：[faster.h:286](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L286)、[faster.h:403](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L403)。

## 3. 会话、操作和返回值

### 3.1 线程会话

| 签名 | 参数、输出与约束 |
|---|---|
| `Guid StartSession(Guid guid = Guid())` | 空 GUID 时创建新 GUID；只允许 REST 阶段；初始化当前线程执行上下文并进入 epoch 保护。 |
| `uint64_t ContinueSession(const Guid& guid)` | 从恢复得到的会话集合继续，返回该会话持久化序列号；未知 GUID 抛 `invalid_argument`，非 REST 抛 `runtime_error`。 |
| `void Refresh(bool from_callback = false)` | 推进 epoch 和检查点等特殊阶段；普通用户使用默认参数。无活跃会话仅有警告并非完整防误用机制。 |
| `void StopSession()` | 推进并等待当前会话挂起请求及阶段完成，退出索引会话和 epoch 保护。 |
| `bool CompletePending(bool wait = false)` | 轮询设备和索引完成队列、处理当前/前一执行上下文和重试；`false` 做一轮后返回是否完成，`true` 持续循环到完成。不是持久化提交 API。 |
| `void CompletePendingCompactions()` | 等待已调度的自动压缩；会话受保护时也推进挂起请求。 |

证据：[faster.h:696](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L696)、[faster.h:766](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L766)、[faster.h:948](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L948)。执行上下文通过 `Thread::id()` 索引，线程槽上限是 **96**，不能将旧注释中的 64 当成当前限制。[thread.h:18](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/thread.h#L18)。

迁移时必须显式处理会话的线程归属：当前会话不是可任意迁移到线程池其他线程的对象。长时间不调用 `Refresh` / `CompletePending` 的活跃线程可能阻碍协作阶段推进。

### 3.2 单键操作签名

```cpp
template<class RC>
Status Read(RC& context, AsyncCallback callback, uint64_t monotonic_serial_num,
            bool abort_if_tombstone = false);
template<class UC>
Status Upsert(UC& context, AsyncCallback callback, uint64_t monotonic_serial_num);
template<class MC>
Status Rmw(MC& context, AsyncCallback callback, uint64_t monotonic_serial_num,
           bool create_if_not_exists = true);
template<class DC>
Status Delete(DC& context, AsyncCallback callback, uint64_t monotonic_serial_num,
              bool force_tombstone = false);
```

| 操作 | 输入和实际输出 | 特殊语义 |
|---|---|---|
| `Read` | key 由上下文提供；值由 `Get` / `GetAtomic` 写到上下文，方法返回状态 | 不存在返回 `NotFound`；`abort_if_tombstone=true` 时墓碑可返回 `Aborted`，供 F2 区别“删除”与“热层没有”。 |
| `Upsert` | 上下文负责写值；返回状态 | 可变区可以尝试原位原子写；不能原位写则追加。`Ok` 不代表本次操作已持久化。 |
| `Rmw` | 上下文定义初值、原位修改、拷贝修改；返回状态 | 默认不存在就创建；`create_if_not_exists=false` 时不存在可返回 `NotFound`。 |
| `Delete` | 上下文提供 key 与记录值大小；返回状态 | 默认路径可能返回 `NotFound`；`force_tombstone` 用于确保产生遮蔽旧数据的墓碑语义。 |

证据：[faster.h:792](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L792)、[faster.h:837](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L837)、[faster.h:872](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L872)、[faster.h:912](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L912)。

各方法将调用者的 `monotonic_serial_num` 写入当前执行上下文。源代码不是替用户自动分配序号，也未在这些入口强制检查单调递增；恢复依赖应用正确维护其操作序列。四类上下文的 `value_t` 必须与 store 值类型满足继承关系及相同对齐，入口用 `static_assert` 检查。

### 3.3 状态与异步所有权

| `Status` 数值 | 解释 |
|---|---|
| `Ok = 0` | 此次操作成功，不等于持久化完成。 |
| `Pending = 1` | 已进入异步完成协议；最终结果经回调返回。 |
| `NotFound = 2` | 不存在，适用于读取及相关条件更新路径。 |
| `OutOfMemory = 3` | 如上下文深拷贝分配失败。 |
| `IOError = 4` | I/O 错误状态。 |
| `Corruption = 5` | 如恢复时索引与日志检查点版本不一致。 |
| `Aborted = 6` | 按 API 可能是状态冲突、墓碑条件终止或 F2 恢复失败归并；不能统一解释为事务回滚。 |

完整枚举：[status.h:10](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/status.h#L10)。并非每个操作都正常返回表中每一项；异常和断言也是当前错误模型的一部分。

回调签名为 `void (*AsyncCallback)(IAsyncContext* ctxt, Status result)`。同步完成直接读取原上下文和返回状态，不应等待同步操作再触发一次回调。发生异步时，框架深拷贝上下文到堆上，回调收到的是持续存活的副本；原栈对象不是异步结果容器。回调通常通过 `CallbackContext<C>` 管理副本释放，继续异步时其 `async` 标志影响所有权移交。`IAsyncContext::DeepCopy_Internal` 必须深拷贝父上下文和被引用数据的必要所有权。证据：[async.h:35](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/async.h#L35)、[async.h:96](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/async.h#L96)、[faster.h:987](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L987)。

## 4. 用户提供的键、值与操作上下文

这是泛型引擎，不是传入任意 `std::string` 就自动序列化的容器。用户需要提供以下编译期协议。

| 扩展点 | 最小关键成员 / 语义 |
|---|---|
| 所有操作上下文 | 继承 `IAsyncContext`；声明 `key_t`、`value_t`；提供 `key()` 与 `DeepCopy_Internal`。 |
| 键 / 浅键 | `size()`、`GetHash()`、与存储键比较的 `operator==`。普通键通过放置构造复制进日志；浅键提供 `write_deep_key_at(dst)` 写入真正持久存储键。 |
| Read 上下文 | `Get(const value_t&)` 与 `GetAtomic(const value_t&)`，读取输出留在上下文或其安全拥有的结果对象。 |
| Upsert 上下文 | `value_size()`、`Put(value_t&)`、`bool PutAtomic(value_t&)`；原子路径返回是否成功。 |
| Rmw 上下文 | `value_size()`、`value_size(const value_t&)`、`RmwInitial(value_t&)`、`RmwCopy(const value_t&, value_t&)`、`bool RmwAtomic(value_t&)`。 |
| Delete 上下文 | `value_size()`；删除记录仍需正确的记录布局信息。 |
| 值类型 | 必须符合日志记录的尺寸、对齐和访问规则；原子访问正确性由用户操作实现承担，不能将无同步的普通写伪装为 `PutAtomic`。 |

证据：[internal_contexts_f2.h:19](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/internal_contexts_f2.h#L19)、[internal_contexts_f2.h:87](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/internal_contexts_f2.h#L87)、[internal_contexts.h:297](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/internal_contexts.h#L297)、[internal_contexts.h:388](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/internal_contexts.h#L388)、[internal_contexts.h:477](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/internal_contexts.h#L477)。

可读范例：计数增量与读上下文位于 [sum_store.h:73](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/playground/sum_store-dir/sum_store.h#L73)；定长与变长键分别位于 [test_types.h:16](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/test/test_types.h#L16)、[test_types.h:162](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/test/test_types.h#L162)。这些是源代码范例证据，不代表本次运行通过。

## 5. 持久化、维护与输出

| 签名 | 返回与完成协议 |
|---|---|
| `bool Checkpoint(IndexPersistenceCallback, HybridLogPersistenceCallback, Guid& token)` | 返回是否成功启动完整检查点；成功时生成 token。`true` 不是已经落盘。 |
| `bool CheckpointIndex(IndexPersistenceCallback, Guid& token)` | 仅索引检查点；有其他系统操作时可返回 `false`。 |
| `bool CheckpointHybridLog(HybridLogPersistenceCallback, Guid& token)` | 仅日志检查点；需要会话继续推进阶段。 |
| `Status Recover(const Guid& index_token, const Guid& hybrid_log_token, uint32_t& version, std::vector<Guid>& session_ids)` | 同步恢复；成功填充版本和可恢复会话 GUID；入口先清空输出。系统动作冲突返回 `Aborted`，索引/日志版本不等返回 `Corruption`。 |
| `bool Compact(uint64_t untilAddress)` | 使用临时 FasterKv 和日志扫描识别活记录并搬到尾部；此方法末尾直接返回，不自动调用截断。 |
| `bool CompactWithLookup(uint64_t until_address, bool shift_begin_address, int n_threads = 8, bool to_other_store = false, bool checkpoint = false)` | 查索引的多线程压缩，调用内等待工作线程以及所请求的后续阶段；要求活跃会话，地址介于 begin 与 safe-read-only 之间；`to_other_store` 依赖配对存储，不能独立开启。 |
| `bool ShiftBeginAddress(Address, GcState::truncate_callback_t, GcState::complete_callback_t)` | 启动 GC、更新逻辑 begin，后续通过回调分别通知截断和索引回收；不是只复制活数据的压缩操作。 |
| `bool GrowIndex(GrowCompleteCallback)` | 争用全局阶段并启动扩容；返回是否启动，回调给新大小。 |

证据：[faster.h:3307](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L3307)、[faster.h:3436](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L3436)、[faster.h:3495](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L3495)、[faster.h:3592](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L3592)、[faster.h:4287](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L4287)、[faster.h:4376](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L4376)。

持久化回调为 `void(Status)` 与 `void(Status, uint64_t persistent_serial_num)`；日志回调在参与会话的持久化阶段执行，序号来自此前执行上下文。GC 回调分别为 `void(uint64_t offset)`、`void()`；扩容回调为 `void(uint64_t new_size)`。证据：[checkpoint_state.h:21](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/checkpoint_state.h#L21)、[faster.h:3204](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L3204)、[gc_state.h:21](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/gc_state.h#L21)、[grow_state.h:15](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/grow_state.h#L15)。

文件系统设备在根路径下使用 `index-checkpoints/<GUID>/` 和 `cpr-checkpoints/<GUID>/`。索引包含 `ht.dat`，日志元数据含 `info.dat`，另有会话上下文持久化。源代码存在 `snapshot.dat` 路径，但当前 `fold_over_snapshot` 是编译期固定 `true`，不能将独立快照文件模式写成默认可配置能力。证据：[file_system_disk.h:496](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/device/file_system_disk.h#L496)、[mem_index.h:936](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/index/mem_index.h#L936)、[faster.h:2518](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L2518)、[faster.h:439](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L439)。Rust 是否兼容这些二进制文件需要单独决策，不能仅靠沿用 API 名称保证。

诊断接口中，`Size()` 返回 `tail_address - begin_address`，**是日志地址跨度而非有效键数量**；`DumpDistribution()` 输出索引分布；`NumActiveSessions()`、`AutoCompactionScheduled()`、`HlogMaxSizeReached()` 返回状态。`STATISTICS` 下另外暴露 `EnableStatsCollection()`、`DisableStatsCollection()`、`PrintStats()`。证据：[faster.h:260](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L260)、[faster.h:490](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/faster.h#L490)。

## 6. F2Kv 的额外 API 与差异

`F2Kv<K,V,D,HHI=MemHashIndex<D>,CHI=ColdIndex<D>>` 的构造参数包含热/冷两组索引配置、日志内存预算和路径；默认热可变比例 `0.6`、冷比例 `0`。也可以传入两组 `FasterKvConfig`。它公开 `hot_store` 与 `cold_store`，并总是启动一个后台状态工作线程。默认 F2 热冷自动压缩均开启，预算分别为 1 GiB 与 8 GiB。证据：[f2.h:19](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L19)、[f2.h:42](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L42)、[config.h:75](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/config.h#L75)。

| F2 公共入口 | 与单层实现的差异 |
|---|---|
| `Guid StartSession()` | 为热冷两层启动同一个新 GUID；没有允许调用者指定 GUID 的该重载。 |
| `uint64_t ContinueSession(const Guid&)` | 两层都继续，返回热层序号。 |
| `StopSession()`、`Refresh()`、`CompletePending(bool=false)` | 协调两层，以及额外的 RMW 重试队列。 |
| `Read(RC&, AsyncCallback, uint64_t)` | 先热后冷；热层墓碑转为最终 `NotFound`，不再回冷层读旧值；冷层命中可插入热读缓存。 |
| `Upsert(UC&, AsyncCallback, uint64_t)` | 转交热层。 |
| `Rmw(MC&, AsyncCallback, uint64_t)` | 热层 RMW、冷层读取、热层条件插入及必要重试构成组合操作。 |
| `Delete(DC&, AsyncCallback, uint64_t)` | 调用热层 `Delete(..., true)`，强制墓碑遮蔽冷层旧数据。 |
| `bool Checkpoint(HybridLogPersistenceCallback, Guid& token, bool lazy=true)` | 后台协调两层检查点；默认允许 2 秒延迟窗口；冲突时抛异常，不是单层的返回 `false` 行为。 |
| `Status Recover(const Guid& token, uint32_t& version, std::vector<Guid>& session_ids)` | 同 token 恢复两层；当前实现对底层失败归并成 `Aborted`。见下方静态问题。 |
| `bool CompactHotLog(uint64_t until_address, bool shift_begin_address, int n_threads=8)` | 将有效热记录迁移到冷层。 |
| `bool CompactColdLog(uint64_t until_address, bool shift_begin_address, int n_threads=8)` | 冷层内部压缩。 |
| `CompletePendingCompactions()`、`Size()`、`DumpDistribution()`、`NumActiveSessions()`、`AutoCompactionScheduled()` | `Size` 是两层日志跨度之和；会话数取两层最大值；其余协调两层。 |
| 条件配置和统计接口 | `TOML_CONFIG` 下 `FromConfigString` / `FromConfigFile`；`STATISTICS` 下统计控制与输出。 |

证据：[f2.h:90](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L90)、[f2.h:229](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L229)、[f2.h:303](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L303)、[f2.h:409](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L409)、[f2.h:577](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L577)、[f2.h:619](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L619)、[f2.h:731](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L731)。

**静态阅读发现，待验证：** `F2Kv::Recover` 的 `version` 引用参数没有赋值；底层任一层恢复失败会在将 F2 阶段复位前直接返回，留下 `RECOVER`；两层版本不一致只记警告。上述是可定位的源码行为，不是已复现测试失败，也不应作为需要原样复制到 Rust 的理想契约。证据：[f2.h:695](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/f2.h#L695)。

## 7. 扫描、设备和平台扩展

`LogRecordIterator<F>` 构造签名为 `(hlog_t* log, Buffering mode, Address begin, Address end, disk_t* disk)`，`GetNext(Address& record_address)` 同时返回记录指针和地址，也有无地址输出的 `GetNext()`。缓冲选项是 `UN_BUFFERED`、`SINGLE_PAGE`、`DOUBLE_PAGE`；同文件还包含页级以及并发页级迭代器。它读取日志物理记录，不能自动解释为去重、排除墓碑的逻辑快照。返回指针关联日志/缓冲存活期，Rust 需重新表达其借用边界。证据：[log_scan.h:784](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/log_scan.h#L784)、[log_scan.h:834](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/log_scan.h#L834)。

设备是模板协议：提供文件/日志文件类型、文件创建、检查点路径和目录、I/O 完成推进；文件核心接口包括 `Open` / `Close` / `Delete`、`ReadAsync(source,dest,length,callback,context)`、`WriteAsync(source,dest,length,callback,context)`、`alignment()`、`Truncate(...)`。I/O 回调签名为 `void(IAsyncContext*, Status, size_t bytes_transferred)`，区别于键值操作回调。`FileSystemFile` 自身的 `Truncate` 是仅回调的空操作，实际日志分段层负责相应回收，不能假设每一层都执行物理截断。证据：[file_system_disk.h:25](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/device/file_system_disk.h#L25)、[async.h:24](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/core/async.h#L24)。

当前平台头文件 `_WIN32` 选择 Windows，否则选择 Linux 文件实现；没有看到独立 macOS I/O 实现。因此本次在 macOS 上能阅读代码不等于原生构建和 I/O 运行支持。`USE_URING` 默认关闭；`USE_BLOBS` 默认关闭，开启后关联 Azure 依赖及示例。TOML 入口受 `TOML_CONFIG` 控制，在当前常用 Debug/Release 配置中启用。证据：[file.h:6](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/src/environment/file.h#L6)、[CMakeLists.txt:10](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/CMakeLists.txt#L10)、[CMakeLists.txt:29](https://github.com/microsoft/FASTER/blob/321d872eabda6a0345c8bd76419f89723ed864ae/cc/CMakeLists.txt#L29)。

## 8. 典型调用流程与迁移验收输入

### 常规单线程会话

1. 定义可存储键值及四类上下文，选择设备、索引与日志预算，构造 store。
2. 当前线程调用 `StartSession()`，保留会话 GUID。
3. 按应用序号调用 `Upsert` / `Read` / `Rmw` / `Delete`。若返回同步结果，立即处理状态及原上下文输出；若 `Pending`，由完成回调处理副本输出。
4. 循环中定期 `Refresh()` 和 `CompletePending(false)`，结束前 `CompletePending(true)`。
5. 若需要可恢复进度，启动检查点，并持续推进到持久化回调成功，记录 token 和持久化序号；只完成挂起 I/O 不替代此步骤。
6. `StopSession()`，最后销毁实例。

### 恢复继续执行

1. 构造使用相同持久化数据布局与设备路径的实例，在恢复完成前不启动业务读写会话。
2. `Recover(index_token, hybrid_log_token, version, session_ids)` 并检查最终状态。F2 使用单 token 入口，但需先明确上文静态问题的处理方式。
3. 为待继续的 GUID 调用 `ContinueSession`，取得已持久化序号。
4. 应用从该恢复边界决定哪些操作需要重放，并从正确的后继序号继续；不能仅依据进程崩溃前客户端最后一次 `Ok` 判断已持久化。

### 后续 Rust 验收应保留的行为矩阵

- 同一业务操作在内存同步完成、日志磁盘 `Pending`、索引磁盘 `Pending` 三条路径上输出一致。
- 读不存在 / 墓碑；RMW 不存在是否创建；F2 删除冷层已有键后不能读到旧值。
- 变长键、浅键写入、值增长、原子更新失败回退；异步上下文不依赖原栈数据继续存活。
- 多线程会话与检查点、扩容、压缩请求竞争或交错时能够推进，退出会话前能收齐结果；单个存储的顶层系统动作仍按状态机串行协调。
- 恢复输出 token、版本、GUID、持久化序号可用于正确重放；错误恢复后对象状态可解释。
- `Size`、扫描记录数、有效键数分别验证，避免指标和逻辑 API 混用。

这些是从现有契约提取的验收需求，不代表本次已经编写或执行了相应测试，也不预先决定 Rust 的接口设计。
