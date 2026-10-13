# 按 FASTER C++ 契约重写 RasterKV：实施计划

日期：2026-10-12。状态：已确认，执行中。决策记录见 [ADR 0001](adr/0001-adopt-faster-cpp-contract.md)。

## 0. 目标与总体策略

**目标：** Rust 版本在相同负载下性能不低于 FASTER C++（固定提交 `321d872eabda6a0345c8bd76419f89723ed864ae`）。驻留负载吞吐 ≥ C++ 的 100%，抽样 P99 ≤ C++ 的 110%；磁盘、检查点、维护负载后续同样以 C++ 双端对比验收。阶段 A 的中间门槛为每操作指令数 ≤ C++ 的 1.2 倍。

**重写前基线（2026-10-12，内网机 Xeon Silver 4116，Rust `942f11b` 对 C++ `321d872`）：** 18 组驻留负载中 Rust 吞吐仅为 C++ 的 7.4%–22.7%；每操作指令数为 C++ 的 6.2–6.6 倍（Read 单线程 2146 对 348 条）。约 43% 的 Rust 指令来自每操作的 epoch 进出、会话准入、结果额度、Task 和统计；四线程写场景还受 operation stripe RwLock 和记录 MutationGate 的共享写拖累。差距是结构性的，不是局部热点。

**策略：** 不在旧引擎上修补。旧实现已整体删除；在 `src/core/` 下按 FASTER C++ 的模块结构重新实现，功能分阶段补齐。

### 契约（全部按 C++ 执行）

| 项目 | 新契约 |
|---|---|
| Epoch | 会话开始时 Protect，调用方定期 `refresh()`；不 refresh 的会话会阻塞回收和阶段推进 |
| 操作结果 | 返回 `Status { Ok, Pending, NotFound, Aborted, OutOfMemory, IoError, Corruption }`；Pending 时上下文复制进会话队列，`complete_pending(wait)` 在会话线程回调 |
| 值并发 | 由值类型负责：原位更新只拿到 `&V`（要求 `V: Sync`），通过原子量或用户锁实现，对应 `PutAtomic`/`RmwAtomic`/`GetAtomic` |
| 回调 panic | 热路径不捕获，panic 直接传播，对应 C++ 异常；被打断的记录状态未定义 |
| Serial / 检查点 | CPR：会话 serial 加 Prepare → InProgress → WaitPending → WaitFlush → PersistenceCallback → Rest |
| 地址 / 记录 | 48 位地址（page + offset）；8 字节 RecordInfo（previous 48 位、version 13 位、invalid/tombstone/final）+ 键 + 值，全部内联在页内 |
| 磁盘格式 | 按 FASTER 方式组织日志页与检查点，不兼容旧 v1 文件 |
| 统计 | 只在会话或全局层面计数，热路径不做原子统计 |

## 1. 目标模块结构

| Rust 模块 | 对应 C++ | 要点 |
|---|---|---|
| `core/address.rs` | `address.h` | `Address(u64)`：offset 25 位、page 23 位；`AtomicAddress` |
| `core/epoch.rs` | `light_epoch.h`、`thread.h` | 按缓存行对齐的槽表，槽号由会话持有；`protect` 一次 SeqCst store；256 槽 drain 列表 |
| `core/record.rs` | `record.h` | 8 字节原子 `RecordInfo`；键值按对齐内联 |
| `core/kv.rs` | Key/Value/Context 契约 | `Key`、`Value` 与四种操作上下文 trait；Pending 通过 `Clone` 做 deep copy |
| `core/hlog.rs` | `persistent_memory_malloc.h` | tail 用 `fetch_add`；页环大小为 2 的幂；head / read_only / safe_read_only / safe_head / flushed 边界；页状态机 |
| `core/index.rs` | `mem_index.h`、`hash_bucket.h`、`hash_table.h`、`malloc_fixed_page_size.h` | 64 字节桶（7 项 + overflow）；tentative 协议；CAS 发布；双版本表 |
| `core/store.rs` | `faster.h` | Internal{Read, Upsert, Rmw, Delete}、BlockAllocate、TraceBack、Refresh、CompletePending、HeavyEnter |
| `core/session.rs` | `ExecutionContext` | serial、phase、version、retry/pending 队列，全部为会话独占的普通字段 |
| `core/checkpoint.rs`、`recovery.rs` | `checkpoint_state.h`、`state_transitions.h`、`recovery_status.h` | CPR 阶段机，支持 fold-over 和 snapshot，恢复 |
| `core/compact.rs`、`gc.rs`、`grow.rs`、`scan.rs`、`read_cache.rs` | 对应 C++ 文件 | 照 C++ 逻辑移植 |
| `device/` | `device/*` | Null、文件、线程池后端，扇区对齐异步读写加回调 |

**`unsafe` 只允许出现在三处**，每处都附安全说明：hlog 页内存与 `get(addr)`（生命周期受 epoch 约束）、记录发布前的初始化、epoch 回收动作。

C++ 依赖未定义行为或"x86 恰好成立"的写法，改为合法原子操作：epoch 槽用 AtomicU64，`protect` 时加 SeqCst 屏障；记录头用 AtomicU64，invalid/tombstone 位用 `fetch_or`；页指针数组用 `AtomicPtr`；可变区的值只通过 `&V` 访问。

## 2. 分阶段实施

### 阶段 A：驻留内核（性能主战场）
1. address、epoch、record、kv trait、hlog（内存部分 + NullDevice）、index、session；store 在 REST 阶段的 Read/Upsert/RMW/Delete、Refresh、`complete_pending` 骨架。
2. `examples/parity.rs` 改用新 API，业务语义与 `tools/performance/memory.cc` 逐行对应。
3. 移植 C++ `cc/test/in_memory_test.cc` 的用例为集成测试；epoch 和索引 CAS 协议加并发压力测试，Linux 上跑 TSAN。
4. **验收**：内网机 18 组负载，每操作指令数 ≤ C++ 的 1.2 倍，吞吐 ≥ 0.9 倍。不达标则先按函数归因收敛，不进入阶段 B。
   - 热路径要求：不取锁、不分配、不克隆 Arc；仅在发布点使用 Acquire/Release；会话借用 store；按 Key/Value 泛型单态化。

### 阶段 B：磁盘与 Pending
页 flush（异步多页写）、head 前移、页关闭与复用（epoch 动作）、冷读（扇区对齐加 buffer pool）、Pending（context 复制、pending_ios、retry_requests、RMW 模糊区的 RETRY_LATER）。移植 C++ paging 测试。**验收**：超内存工作集（均匀/Zipf）双端对比，吞吐 ≥ 0.9 倍。

### 阶段 C：检查点与恢复
CPR 阶段机、索引检查点、日志 fold-over/snapshot、会话 serial 持久化、Recover、ContinueSession。移植 C++ recovery 测试，并加逐 I/O 中断的崩溃注入。**验收**：带检查点的读写吞吐、检查点耗时、恢复耗时与 C++ 对比。

### 阶段 D：维护功能
GrowIndex、ShiftBeginAddress、Compact（含 ConditionalInsert/lookup 版本）、LogIterator 扫描、ReadCache、自动压缩。移植对应 C++ 测试，并做双端对比。

### 阶段 E：收尾
完善配置与诊断、文档、示例；按需补充 io_uring 等后端。

## 3. 测量与验收流程

- 只在内网机执行，源码打包上传到 `/tmp/raster-<阶段>/`。C++ 每次从固定提交重新构建。
- 每场景预热后 3 轮 AB/BA 交替，每进程 200 万次操作；单线程绑 CPU 4，四线程绑 CPU 4–7，内存绑节点 0。两端 checksum/digest 必须完全一致。
- 同时采集 `perf stat` 的每操作指令数和周期数；未达标时用 `perf record` 做函数级归因。
- 每阶段运行全部 `cargo test`，包括移植过来的 C++ 测试。

## 4. 风险

- 阶段 A 必须单独达标后再继续。如未达标，先区分是代码生成问题（边界检查、未内联、`Result` 搬运）还是结构问题。
- Rust 合法性带来的额外成本（epoch 屏障、原子记录头读取）预计每操作仅几条指令，在 1.2 倍预算之内。
- 旧 v1 数据文件不再可读。

## 5. 进展记录

测量环境均为内网机（2×Xeon Silver 4116，Ubuntu 24.04），C++ 为固定提交从源码构建；单线程绑 CPU 4，四线程绑 CPU 4–7，内存绑 NUMA 节点 0；两端业务结果（checksum/digest 或恢复后求和）逐次核对一致。

### 阶段 A：驻留内核（已完成）

- 18 组驻留负载吞吐为 C++ 的 1.9–4.4 倍；每操作指令数约为 C++ 的 0.5–0.7 倍（重写前为 6.2–6.6 倍）。
- 已知偏差：四线程单键 Upsert（upsert/shared-hot/4）的抽样 P99 对代码生成极其敏感，不同构建在 C++ 的 0.83–1.16 倍之间摆动；同一构建内稳定。RMW 同场景稳定在 0.84–0.92 倍。最终验收时复测。

### 阶段 B：磁盘与 Pending（已完成，真实高速磁盘对比待补）

- 超内存负载（1600 万键、256 MiB 日志窗口、约 44% 读盘）在 tmpfs 上、两端均使用带缓存 I/O：Read/RMW 单线程与四线程吞吐为 C++ 的 2.3–2.5 倍，P99 为 0.90–0.92 倍。
- 内网机只有机械盘级别存储（约 2100 随机读 IOPS），O_DIRECT 真实磁盘对比待有高速磁盘的机器后补测。
- 模糊区 RMW 已按 C++ 的 RETRY_LATER 语义进入重试队列。

### 阶段 C：检查点与恢复（fold-over 已完成）

- CPR 状态机、检查点锁、模糊索引检查点、fold-over 日志检查点、会话 serial 持久化、恢复与 `continue_session` 已实现。
- 四会话超内存并发 RMW 下的前缀一致性测试连续 300 次通过；过程中发现并修复了 serial 记录时机、Pending RMW 绕过 CPR 检查、页帧复用清零竞态、部分扇区写回乱序等问题。
- 四线程 RMW 负载下（400 万键、4000 万次操作，tmpfs），7 轮中位数：整体吞吐 1.61 倍、检查点期间吞吐 1.45 倍、检查点耗时 0.86 倍、恢复耗时 0.72 倍（相对 C++）。
- 未做：snapshot 模式日志检查点；与 C++ 的两处差异——恢复后版本号继续为 v+1（C++ 会回到 0），记录版本按 13 位取模比较。

### 阶段 D：维护功能（已完成）

- 已实现日志扫描（LogIterator）、ShiftBeginAddress 与索引 GC、GrowIndex、CompactWithLookup（含 ConditionalInsert）、后台自动压缩和读缓存，均有并发测试覆盖。
- 压缩：1600 万键、覆盖一半后压缩 416 MiB 日志（tmpfs，四线程），3 轮：C++ 约 7.5 s，Rust 约 1.97 s（约 3.8 倍），两端压缩字节数、压缩后日志大小和校验和一致。
- 读缓存：hotread 负载（1600 万键，九成读落在最早写入、已落盘的 1/32 键上，tmpfs），读缓存 256 MiB 时单线程吞吐为 C++ 的 1.88 倍、四线程 2.07 倍，P99 为 0.91–0.93 倍；两端 Pending 数几乎相同，说明命中行为一致。未开读缓存时同负载为 2.34–2.56 倍。
- 读缓存不拖累驻留快路径：REST 且无读缓存的会话走编译期特化路径，其余变体放在独立函数中，读缓存以 `Box` 存放以免挤开存储的热字段。未这样处理时实测有 5–12% 的吞吐回退，主要来自字段布局变化而非指令数。
- 取舍：把读缓存读路径也内联进调用方可使开启读缓存的 hotread 再快约 6%，但会使无读缓存的驻留读慢约 4%。默认配置不开读缓存，因此优先保证后者。
- 与 C++ 的差异：
  - 索引检查点在序列化时把读缓存项换成其后的日志地址，不修改在线索引（C++ 会剔除在线索引中的读缓存项）。
  - GrowIndex 分裂时丢弃读缓存项。C++ 中 Grow 与读缓存同时使用不安全。
  - 读缓存命中时要求其后的日志地址不低于 begin，截断后不再返回被截断键的缓存值。
  - 读缓存驱逐由一把锁串行执行，C++ 为多线程协作执行。
  - 自动压缩必须指定日志大小预算，压缩点按页对齐。
- 地址第 47 位用作读缓存标记，日志页号上限为 2^22，与 C++ 一致。
