# RasterKV 公开接口

更新日期：2026-09-12。本文对应当前源码；精确签名以 [api](../src/api/mod.rs)、[schema](../src/schema/mod.rs)、[device](../src/device/mod.rs) 和 [types](../src/types/mod.rs) 为准。P0—P8 已验收，P9 总验收仍进行中。初版草案见 [固定历史版本](https://github.com/wayslog/raster/blob/52c0657661edc319e5e6bc4af2ab7518d88733f6/docs/10-RasterKV公开接口.md)。

## 1. 存储、创建与恢复

`RasterKV<S: Schema>` 可 Clone 共享同一 Engine；业务通过线程绑定的 Session 执行。Builder 必须显式提供 DeviceFactory。下表省略接收者，参数及返回类型与实际入口对应。

| 入口 | 返回 | 契约 |
| --- | --- | --- |
| `RasterKV::builder(schema: S)` | `Builder<S>` | 收集 Schema、默认 Config 和设备工厂 |
| `Builder::config(Config)` / `device(Box<dyn DeviceFactory>)` | `Self` | 链式替换配置/设备 |
| `Builder::create()` | `Result<RasterKV<S>, Error>` | 准备全部组件后发布；拒绝覆盖已有存储材料 |
| `Builder::recover(RecoverySet)` | `Result<(RasterKV<S>, RecoveryReport), Error>` | 校验材料并重放到独立工作目录，完成才发布 |
| `id()` | `StoreId` | 恢复后保持持久身份 |
| `start_session(SessionOptions)` | `Result<Session<S>, Error>` | 每线程每实例至多一个活跃会话 |
| `continue_session(SessionId)` | `Result<ResumedSession<S>, Error>` | 激活恢复身份；未知、仍活跃或线程名额冲突时拒绝 |
| `maintenance()` | `Maintenance<S>` | 共享维护句柄，不代替业务会话推进 |
| `scan(ScanOptions)` | `Result<RecordScanner<S>, Error>` | 创建物理扫描 |
| `shutdown(Deadline)` | `Result<ShutdownReport, Error>` | 停止自动维护并排空；活跃会话、扫描或手动动作可返回 Busy |

SessionOptions 的 `id: Option<SessionId>` 默认 None。ResumedSession 含 session/progress；progress 是检查点持久化边界，同实例关闭重续后的 `last_accepted()` 可能更大，新序号须超过当前接受进度。

ShutdownReport 含 device_drained，成功不自动创建检查点。关闭超时可续等，Busy 后先关闭/推进现有对象；diagnostics.active_session_ids 提供活跃身份。

## 2. 四种操作与选项

操作实现 `Keyed<S>: 'static`，提供 `key(&self) -> &KeyOf<S>`；每种操作自定义 `Output: 'static`。操作与输出不要求 Send，可使用 Rc，必须拥有挂起所需的数据。完整签名见 [operation.rs](../src/api/operation.rs)。

| trait | 必需方法 | 行为 |
| --- | --- | --- |
| ReadOperation | `read(&mut self, ValueRead<'_, S>) -> Result<Output, Error>` | 从受保护值生成输出 |
| UpsertOperation | replacement、update_in_place | replacement 返回 `(OwnedValueOf<S>, Output)`；原地返回 UpdateDecision |
| RmwOperation | initial、copy_update、update_in_place | initial 无值参数，copy_update 接收 ValueRead，二者返回 `(OwnedValueOf<S>, Output)`；原地接收 ValueUpdate |
| DeleteOperation | `complete(self, DeleteOutcome) -> Output` | 删除生效后消费操作生成结果 |

replacement/initial/copy_update 的接收者为 `&mut self`，结果包装在 `Result<_, Error>` 中。update_in_place 返回 `Result<UpdateDecision<Output>, Error>`，UpdateDecision 为 Updated(T) 或 Append。计算可能因条件发布冲突重做，不应带不可重放的外部副作用；已经生效的修改和删除通知不重放。

| Session 方法 | 选项默认 | 正常结果 |
| --- | --- | --- |
| `read(serial, request, ReadOptions)` | abort_if_tombstone=false | 命中 Success；缺失 NotFound；墓碑默认 NotFound，选项开启为 Aborted(Tombstone) |
| `upsert(serial, request)` | 无额外选项 | 覆盖或创建，用户输出包装为 Success |
| `rmw(serial, request, RmwOptions)` | create_if_missing=true | 缺失可初始化；禁止创建则 NotFound |
| `delete(serial, request, DeleteOptions)` | force_tombstone=false | 默认缺失或已有墓碑为 NotFound；强制选项允许写墓碑遮蔽 |

四种提交均返回 `Result<Submission<Output>, Rejected<Request>>`。同一会话 Serial 必须严格递增但可跳号；接受前拒绝不消费序号。DeleteOutcome 声明 TombstoneWritten/IndexRemoved，当前成功删除实现交付 TombstoneWritten，不能假定已经存在索引移除优化。

上游有路径相关的缺失删除成功及墓碑读回缺失差异；A02/A05 的最终选择尚待完成，见 [一期验收报告](acceptance/一期验收报告.md)。

## 3. 接受、结果与票据

```text
Result<Submission<T>, Rejected<R>>
├─ Err：未接受，归还 request 和 reason，不消费序号
└─ Ok：已经接受
   ├─ Ready(OperationResult<T>)：即时终结
   └─ Pending(Ticket<T>)：由原会话继续推进

OperationResult<T> = Result<Outcome<T>, OperationError>
Outcome<T> = Success(T) | NotFound | Aborted(AbortReason)
OperationError = { cause: Error, effect: NotApplied | Applied | Unknown }
```

NotFound/Aborted 是已接受请求的结果，不能当成拒绝。AbortReason 包含 Tombstone 与 ConditionNotMet；后者目前没有四操作的常规返回路径。接受后失败仍消费序号；Applied/Unknown 不能自动重放。

Ticket 不借用 Session，不可跨线程，可跨会话关闭保留结果。`Ticket::id()` 返回 RequestId；`try_take(&mut self)` 返回 `Result<TicketState<T>, TicketError>`，状态为 Pending 或 Ready(OperationResult)。收取后再次收取为 AlreadyTaken；丢弃票据不取消请求。

未收取结果受 max_results 约束；同步 Ready 已将结果移交调用方，不占用挂起结果槽。Pending 收取或票据/完成端均释放后归还名额。Session::try_take 额外检查存储及会话身份。TicketError 包含 WrongSession、AlreadyTaken、AlreadyCompleted、BorrowConflict；没有公开 Completer 或另一套完成观察器 API。

## 4. 推进和结束会话

| Session 方法 | 返回 | 边界 |
| --- | --- | --- |
| `id()` / `last_accepted()` | SessionId / Option<Serial> | 身份和接受进度 |
| `refresh()` / `poll(PollBudget)` | `Result<Progress, Error>` | 观察阶段，按预算推进 |
| `try_take(&mut Ticket<T>)` | `Result<TicketState<T>, TicketError>` | 校验身份并尝试收取 |
| `wait(&mut Ticket<T>, Deadline)` | `Result<OperationResult<T>, Error>` | 外层等待错误与内层业务失败分别处理 |
| `complete_pending(WaitMode)` | `Result<DrainReport, Error>` | 排空请求，不自动收取票据或完成检查点 |
| `wait_maintenance(&MaintenanceTicket<R>, Deadline)` | `Result<SharedReport<R>, Error>` | 同时推进原会话与维护 |
| `wait_auto_compaction(Deadline)` | `Result<AutoCompactionStatus, Error>` | 推进原会话，等待自动维护空闲/停止 |
| `close(Deadline)` | `Result<CloseReport, Error>` | 排空并注销，超时可续等；报告 session/drained |

PollBudget 包含 NonZeroUsize，默认64；Deadline 使用 Instant 单调时钟。WaitMode 为 Once/Until，DrainReport 为 Pending(Progress)/Drained；Progress 含 completed/remaining/phase_advanced。以上成功都不表示检查点持久化。

等待超时不取消原任务。活跃会话必须继续参与阶段推进；Maintenance::poll 不执行业务回调。未排空 Session 的 Drop 进入失败关闭；遗忘对象可能永久占资源/阻挡进度，不能强制回收仍可能访问的页。

## 5. 检查点与维护

维护启动返回 `Result<MaintenanceTicket<R>, Error>`，成功仅表示接受。票据 id 为 MaintenanceId；`try_report()` 返回 `Result<Option<SharedReport<R>>, Error>`。`SharedReport<R> = Arc<Result<R, Error>>` 可重复观察，与业务票据一次取走不同。

| Maintenance 方法 | R | 行为 |
| --- | --- | --- |
| `checkpoint(CheckpointKind)` | CheckpointReport | Full/Index/Log 显式选择 |
| `grow_index()` | IndexGrowthReport | 当前桶数加倍 |
| `compact(CompactionOptions)` | CompactionReport | ScanDedup/Lookup，支持多工作者 |
| `shift_begin(LogAddress)` | GcReport | 调用方先确保所需最新值已迁移，再截断 |
| `release_checkpoint(CheckpointToken)` | CheckpointReleaseReport | 校验持久依赖，显式失效和删除材料 |

其他入口为 poll、auto_compaction_status、stop_auto_compaction、wait_auto_compaction。停止禁止新任务，已接受任务仍需排空；Idle 只是瞬时空闲，停止并等待才确认线程退出。

| 报告或输入 | 字段与含义 |
| --- | --- |
| RecoverySet | store/index/log；Full 可将同一 token 用于两项，Index+Log 必须实际配对 |
| DurableProgress | session/serial/version，成功检查点保证的恢复边界 |
| CheckpointReport | kind/token/version/begin/end/sessions；Index 不承诺会话持久化 |
| RecoveryReport | set/version/sessions；会话需显式续接 |
| IndexGrowthReport | old_buckets/new_buckets/generation；恢复配置匹配材料容量 |
| CompactionOptions | algorithm/until/workers/shift_begin/checkpoint 全部显式，无默认 |
| CompactionReport | until/copied/gc/checkpoint；可选后续顺序为检查点后截断 |
| GcReport | begin/index_cleaned/deleted_segments/physical；逻辑生效与物理删除分开 |
| CheckpointReleaseReport | token/retirement/confirmed_absent_materials/physical；确认缺失数不等于新删除数 |
| PhysicalReclamation | Completed、DeferredByRuntime{begin,end}、DeferredByRecoverySet{blockers,begin,end}；延后也终结本次动作 |
| AutoCompactionStatus | phase/active/完成次数/最近报告/failure/log_bytes/budget_reached；快照拥有自己的报告 |

Index 不能独立恢复完整键空间；Log 须绑定同实例已提交且有效的索引基准。恢复拒绝身份、Schema、算法/种子、格式、配对或材料不符，不静默换哈希或空数据恢复。

Error::CompactionFailed/GcFailed/CheckpointReleaseFailed 保留实际复制量、已完成子报告、逻辑边界、删除确认及 cause。CheckpointRetirement 区分 NotAttempted/PossiblyRetired/Retired，未知失效不能当作仍可恢复。

## 6. 扫描、配置与诊断

ScanOptions 的 begin/end/buffering 必填。Unbuffered 按需当前页，SinglePage 在当前页外预读一页，DoublePage 预读两页；总页上限分别一、二、三页。

`next_record(&mut self) -> Result<Option<ScannedRecord<S>>, Error>` 返回拥有的 address/version/key/value/tombstone/invalid，结束为 None。墓碑或 invalid 无值，不调用值解码；可含旧记录和重复键，扫描不是有效键快照。

截断竞争返回 RangeTruncated。Busy/DeadlineExceeded/OutOfMemory 可重试，其他错误关闭扫描；专家恐慌还失败关闭引擎。close 使用 Config.scan.timeout 等待预读归还，超时可再次调用。

配置完整字段、默认值及四个 TOML 入口见 [15](15-配置与诊断.md)。它们统一使用 Error；config-toml 只控制解析支持，错误不回显 TOML 原文或私人路径。

`diagnostics() -> Result<Diagnostics, Error>` 观察真实边界、会话身份、同步及挂起请求、分配量、扩容和自动维护。`statistics() -> Statistics` 返回拥有计数；`write_statistics(&mut impl Write) -> Result<(), Error>` 使用调用方输出。enable_stats_collection/disable_stats_collection 不清空历史；已采样请求仍记录到终结。观察不是全局事务快照，跨度/桶占用不是有效键数。

## 7. 扩展与实际使用

KeyCodec 定义 Key/OwnedKey、规范编码、完整哈希、format_id/hash_descriptor 及身份检查；OwnedKey 可 Borrow 为 Key。内建 ByteKey 为原始字节，U64Key 为八字节小端，均固定 FNV-1a 64。

安全 ValueCodec 通过 SerializedValue 适配普通值；AtomicU64Value 提供原子值。unsafe ValueLayout 定义拥有值、短期 Read/Update 视图、plan/plan_decode、初始化、稳定编码、拥有解码和销毁。许可由引擎构造，专家必须满足 [11 状态协议](11-RasterKV内存与状态协议.md)。

DeviceFactory::open 返回 `Result<Box<dyn Device>, Error>`。Device::submit 返回 IoId 或归还整个 RejectedIo；poll 交付拥有型 IoCompletion；shutdown 按 Deadline 排空。读写、同步、长度、目录、重命名、删除、锁、关闭和取消显式建模，失败也须归还缓冲。Null/Memory/ThreadPool 可用，Windows/io_uring 后置。

[16 公开流程](16-公开接口使用流程.md)提供真实示例。精确签名可直接生成源码文档：

```sh
cargo doc --locked --all-features --no-deps
cargo run --locked --release --example memory
cargo run --locked --release --example interface
cargo run --locked --release --example disk_lifecycle
cargo run --locked --release --example automatic
```

磁盘示例默认独立临时目录，显式目录必须尚不存在。F2、跨 store 压缩和高级后端后置；原始上游差异、性能限制及一期状态以 [验收报告](acceptance/一期验收报告.md) 和 [计划地图](14-RasterKV第一期实现计划地图.md) 为准。
