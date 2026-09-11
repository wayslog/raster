# RasterKV 公开接口草案

日期：2026-09-10。状态：建议设计；以下 Rust 片段用于表达接口形状，省略私有字段、部分辅助类型和实现体，**不是可直接编译的库代码**。模块关系见 [09](09-RasterKV模块设计.md)，安全与状态条件见 [11](11-RasterKV内存与状态协议.md)。

后续已有可编译接口骨架，见 [13](13-模块基础骨架.md) 和 src/api；本文保留设计草案，代码中的具体化差异在 13 中列出。

## 1. 三组公开对象

P1.1 新增实际入口：StoreId/SessionId/CheckpointToken 的 generate/validate，LogAddress/CacheAddress 的 validate/checked_add/page_offset/from_page_offset，KeyHash::tag，以及 KeyCodec::validate_identity。ByteKey/U64Key 已实现 KeyCodec。参数、返回错误和兼容规则见 [类型与键契约](acceptance/P1.1类型与键契约.md)；下文的引擎接口仍是草案。

```rust
pub struct RasterKV<S: Schema> { /* 共享引擎句柄 */ }
pub struct Session<S: Schema> { /* 线程绑定的业务操作流 */ }
pub struct Ticket<T: 'static> { /* 类型化的一次性结果槽 */ }

pub enum Submission<T: 'static> {
    Ready(OperationResult<T>),
    Pending(Ticket<T>),
}

pub type OperationResult<T> = Result<Outcome<T>, OperationError>;

pub enum Outcome<T> {
    Success(T),
    NotFound,
    Aborted(AbortReason),
}

pub struct Rejected<R> {
    pub request: R,
    pub reason: SubmitError,
}
```

- RasterKV 句柄在 Schema 和内部实现满足安全条件时可 Clone + Send + Sync；关闭状态在所有克隆之间共享。
- Session 和 Ticket 明确 **!Send、!Sync**，可用私有 `PhantomData<Rc<()>>` 等稳定机制约束。`'static` 用户上下文/结果表示不借用短命外部数据，不要求 Send。
- 返回 `Err(Rejected<R>)` 表示请求尚未接受，归还原请求且未消耗序号；`Ok(Submission::...)` 表示已经接受，操作本身也可能最终报错。
- `NotFound` 和条件 `Aborted` 是操作结果，不等同于设备错误。`OperationError` 记录错误类别和 `Effect::{NotApplied, Applied, Unknown}`，不能将所有错误当作可安全重试。

## 2. 创建、恢复和生命周期

```rust
impl<S: Schema> RasterKV<S> {
    pub fn id(&self) -> StoreId;
    pub fn builder(schema: S) -> Builder<S>;

    pub fn start_session(
        &self, options: SessionOptions,
    ) -> Result<Session<S>, SessionError>;

    pub fn continue_session(
        &self, id: SessionId,
    ) -> Result<ResumedSession<S>, SessionError>;

    pub fn maintenance(&self) -> Maintenance<S>;
    pub fn diagnostics(&self) -> Diagnostics;
    pub fn scan(&self, options: ScanOptions) -> Result<RecordScanner<S>, ScanError>;
    pub fn shutdown(&self, deadline: Deadline) -> Result<ShutdownReport, ShutdownError>;
}

impl<S: Schema> Builder<S> {
    pub fn config(self, config: Config) -> Self;
    pub fn device(self, factory: Box<dyn DeviceFactory>) -> Self;
    pub fn create(self) -> Result<RasterKV<S>, OpenError>;
    pub fn recover(
        self, set: RecoverySet,
    ) -> Result<(RasterKV<S>, RecoveryReport), OpenError>;
}
```

`create` 仅用于新存储或明确的非持久化设备，不隐式覆盖已有数据。`recover` 在实例对外可见前同步完成，避免多个 RasterKV 克隆之间出现“某个线程正在恢复，另一个已经写入”的组合。上游实例内 Recover 的功能通过该工厂入口保留，方法形式有意调整。

`RasterKV::id()` 返回持久存储身份，恢复后不变，可与检查点 token 组成 `RecoverySet`。`Config.recovery` 默认限制 1,000,000 条重放记录、128 MiB 索引输入和 300 秒恢复时间；这是恢复资源预算，不是总内存上限。恢复先在独立工作目录安装日志，全部校验和同步成功后才发布实例。

`SessionOptions` 支持可选 GUID；活跃 GUID 不可重复注册。`ResumedSession` 包含 Session 和 `DurableProgress`，该进度来自最近恢复报告；继续不存在或已被占用的会话返回错误。

第一阶段建议每线程每存储最多一个活跃业务 Session，以便匹配原线程上下文语义，重复开始明确拒绝。不同线程共享一个 RasterKV。调用线程不会通过全局 TLS 查到另一个 store 的隐式会话。

Config 分为索引、日志、缓存、维护、恢复预算、会话/请求预算和设备选项。`Config::from_toml_str` / `from_toml_file` 对应原配置入口；解析后必须验证范围。命名、默认值与上游差异要在实施时逐项记录。

## 3. 四种操作及拥有型上下文

```rust
impl<S: Schema> Session<S> {
    pub fn read<R: ReadOperation<S>>(
        &mut self, serial: Serial, request: R, options: ReadOptions,
    ) -> Result<Submission<R::Output>, Rejected<R>>;

    pub fn upsert<U: UpsertOperation<S>>(
        &mut self, serial: Serial, request: U,
    ) -> Result<Submission<U::Output>, Rejected<U>>;

    pub fn rmw<M: RmwOperation<S>>(
        &mut self, serial: Serial, request: M, options: RmwOptions,
    ) -> Result<Submission<M::Output>, Rejected<M>>;

    pub fn delete<D: DeleteOperation<S>>(
        &mut self, serial: Serial, request: D, options: DeleteOptions,
    ) -> Result<Submission<D::Output>, Rejected<D>>;
}
```

四种操作 trait 都继承 `Keyed<S> + 'static`，输出为 `'static`；请求持有其必要输入。请求被移动到方法，同步完成可在栈上处理；Pending 时移动进会话挂起表，取消 C++ 的 DeepCopy 协议。内建操作提供 `ReadValue`、`ReplaceValue`、`Remove`，用户只在投影读取、特定原子更新或 RMW 时定义操作类型。

| trait | 关键方法与输出 | 引擎何时调用 |
| --- | --- | --- |
| `Keyed<S>` | `key(&self) -> &KeyOf<S>` | 哈希与查找；KeyOf 由 Schema 的 KeyCodec 指定 |
| `ReadOperation<S>` | `read(&mut self, ValueRead<'_, S>) -> Result<Output, UserError>` | 找到非墓碑记录后，视图封装稳定或同步访问 |
| `UpsertOperation<S>` | `replacement(&mut self) -> Result<(OwnedValueOf<S>, Output), UserError>`；`update_in_place(&mut self, ValueUpdate<'_, S>) -> Result<UpdateDecision<Output>, UserError>` | 可变区先尝试原地，否则生成替代记录 |
| `RmwOperation<S>` | `initial`、`copy_update(ValueRead)` 返回 `(OwnedValue, Output)`；`update_in_place(ValueUpdate)` 返回 UpdateDecision | 不存在初始化、稳定旧值复制更新、可变区原地更新 |
| `DeleteOperation<S>` | `complete(self, DeleteOutcome) -> Output` | 引擎完成删除后转换用户输出；墓碑布局由 Schema/format 定义 |

```rust
pub enum UpdateDecision<T> {
    Updated(T),
    Append,
}
```

`Append` 表示**未修改旧记录**，请求引擎走追加路径；它不携带在并发旧值上算出的替代值。RMW 追加时重新获得稳定视图，执行 `copy_update`，再以预期索引项发布。若源记录仍可原地更新，须取得覆盖“读取旧值—计算—发布”的独占替换许可；仅比较索引地址不能检测同地址原地修改。冲突释放许可并重新读取计算，具体仲裁见 11 第 2 节。`Updated` 的原子或独占变更就是此次原地更新的生效点，不能在成功后盲目重跑业务修改。

`update_in_place` 返回 Err 的正常契约同样要求未修改旧值。引擎提供的更新能力须跟踪是否发生变更；若调用者已经修改后又报错或 panic，则标记 Applied/Unknown 并进入保守失败关闭，不能自动追加或再次执行 RMW。布局插件和 guard 即使在这条路径上也必须维持内存有效性。

计算型方法可能重试，不能包含外部发消息、付款等不可重复副作用；最终结果交付或观察器才是应用处理外部动作的位置。安全用户 trait 可以破坏业务语义，但不得借此破坏内存安全；实际字节访问由 ValueLayout 的受控能力限定。

`ReadOptions` 保留墓碑中止开关；`RmwOptions` 保留缺失时是否创建；`DeleteOptions` 保留强制墓碑能力。即使 F2 暂缓，也不删除单层操作已经具备的这些条件。

## 4. Ticket、推进与背压

```rust
pub enum TicketState<T> {
    Pending,
    Ready(OperationResult<T>),
}

impl<S: Schema> Session<S> {
    pub fn refresh(&mut self) -> Result<Progress, SessionError>;
    pub fn poll(&mut self, budget: PollBudget) -> Result<Progress, SessionError>;
    pub fn try_take<T: 'static>(
        &mut self, ticket: &mut Ticket<T>,
    ) -> Result<TicketState<T>, TicketError>;
    pub fn wait<T: 'static>(
        &mut self, ticket: &mut Ticket<T>, deadline: Deadline,
    ) -> Result<OperationResult<T>, WaitError>;
    pub fn complete_pending(
        &mut self, mode: WaitMode,
    ) -> Result<DrainReport, WaitError>;
    pub fn close(&mut self, deadline: Deadline) -> Result<CloseReport, CloseError>;
}

impl<T: 'static> Ticket<T> {
    pub fn try_take(&mut self) -> Result<TicketState<T>, TicketError>;
}
```

Ticket 不借用 Session，可以同时持有 `Ticket<String>` 和 `Ticket<u64>`。挂起任务按具体类型保存请求，擦除推进方法；输出槽由原具体任务填充，再由对应 Ticket 取出，不能通过 unchecked downcast 猜测类型。ID 至少包括 store、session、slot 与 generation；错误会话、过期或重复取出返回明确错误。

`Ticket::try_take` 仅取结果，不推进，也不需要 Session 存活；成功 close 后仍可取走保留输出。`Session::try_take` 额外验证票据属于本会话，再委托票据取出。`poll` 有有限预算，推进设备事件路由、当前/旧版本请求、重试和阶段；`refresh` 推进会话观察和安全边界，不承诺排空 I/O。`wait` 使用同一会话不断 poll，超时保留原 Ticket。`complete_pending` 判定已接受任务的执行排空，不要求调用者取走所有结果，也不表示持久化完成。

每会话限制在途任务数及保留结果资源。必须为已接受任务保留完成通知位置，队列满时不得丢 CQE；未接受请求以 Rejected 归还。大输出不能事先精确预算时，对引擎管理的分配显式失败，任意用户自行分配的内存不纳入引擎可强制限制承诺。

丢弃 Ticket 只放弃结果；任务仍运行并清理。结果槽回收带代次，旧 I/O 不能误投新请求。票据不是通用 Future，不会自行推进引擎。

`close` 第一次调用后会话进入 Closing，拒绝新操作；成功表示执行已排空且安全注销。超时仍可用原 Session 继续 poll/close。异常 Drop 不能无限等待其他线程，也不能把 !Send 上下文移到后台；采用 [11](11-RasterKV内存与状态协议.md) 的保守失败关闭规则。

## 5. 调用例

```rust
// 设计示例：具体构造辅助方法以实现时接口测试为准。
let store = RasterKV::builder(ByteSchema::default())
    .config(config)
    .device(Box::new(ThreadPoolDeviceFactory::new(path)))
    .create()?;
let mut session = store.start_session(SessionOptions::default())?;

let submitted = session.read(
    Serial::new(10),
    ReadValue::new("用户:42".as_bytes().to_vec()),
    ReadOptions::default(),
)?;

let result = match submitted {
    Submission::Ready(result) => result,
    Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline)?,
};
match result? {
    Outcome::Success(value) => consume(value),
    Outcome::NotFound => handle_missing(),
    Outcome::Aborted(reason) => handle_abort(reason),
}
session.close(deadline)?;
```

这里 deadline 为调用者提供的截止时间。关闭没有替应用做检查点；需要恢复保证时，应在 close 前完成下一节的持久化流程。示例没有展示应用如何转换 Rejected 和其他错误，实际 examples 必须完整处理。

## 6. 维护、检查点与恢复输出

```rust
impl<S: Schema> Maintenance<S> {
    pub fn checkpoint(
        &self, kind: CheckpointKind,
    ) -> Result<MaintenanceTicket<CheckpointReport>, MaintenanceStartError>;
    pub fn compact(
        &self, options: CompactionOptions,
    ) -> Result<MaintenanceTicket<CompactionReport>, MaintenanceStartError>;
    pub fn shift_begin(
        &self, address: LogAddress,
    ) -> Result<MaintenanceTicket<GcReport>, MaintenanceStartError>;
    pub fn release_checkpoint(
        &self, token: CheckpointToken,
    ) -> Result<MaintenanceTicket<CheckpointReleaseReport>, Error>;
    pub fn grow_index(
        &self,
    ) -> Result<MaintenanceTicket<IndexGrowthReport>, MaintenanceStartError>;
}
```

维护启动冲突返回 Busy，不悄悄启动另一个全局动作。`CheckpointKind::{Full, Index, Log}` 保留三种检查点；`CompactionOptions` 指定 `Algorithm::{ScanDedup, Lookup}`、截止地址、线程数、是否推进 begin、是否串接检查点。复合任务按阶段链执行，不能持有动作锁再等待自己发起的检查点。

维护句柄提供 `status` / `try_report`；不提供脱离会话的无限等待。`Session::wait_maintenance(&ticket, deadline)` 推进自身会话和维护事件；其他活跃线程仍须刷新。维护控制线程可以推进共享 I/O 和无用户上下文的步骤，不能代替某个活跃 Session 伪造阶段确认。无业务会话时，由维护推进器完成全部无参与者步骤。

`Maintenance::poll(budget)` 是显式的无会话维护驱动入口。自动压缩线程调用同一入口；一般维护任务即使没有自动压缩，也可由 Session::poll 或 Maintenance::poll 驱动，不隐含额外全局运行时。

| 报告 | 必须包含的意义 |
| --- | --- |
| CheckpointReport | kind、已完成 token、检查点版本、日志边界；日志/完整检查点包含各会话 DurableProgress |
| RecoveryReport | 实际使用的 RecoverySet、恢复版本、恢复边界、可继续会话及持久化进度 |
| CompactionReport | 已完成扫描/搬运范围、迁移统计、可选 GC/检查点结果；失败保留已经发生的效果 |
| GcReport | begin、index_cleaned、deleted_segments；工作段物理回收为 Completed 或 DeferredByRuntime |
| CheckpointReleaseReport | token、retirement、confirmed_absent_materials；检查点材料回收为 Completed 或带实际引用集合的 DeferredByRecoverySet |
| IndexGrowthReport | 原/新桶数、完成后的表代次；启动返回不等于扩容完成 |

完整检查点可产生可用的 RecoverySet；仅索引检查点不能单独宣称会话 durable。`RecoverySet` 显式包含索引 token 和日志 token，验证 store ID、格式/schema/hash 标识、版本和覆盖关系；不同 token 只有匹配时才能组合。

GC 与检查点显式释放是分别报告的独立动作。延后或普通删除错误终结本次动作，后续显式调用才能重试；没有自动删除队列。DeferredByRecoverySet 表示目标 token 仍受已提交 Log 引用保护，没有释放其材料，也不会永久占用动作。

为保留回调风格，可提供 **可选**的 Pending 完成观察器 `FnOnce(&OperationResult<T>) + 'static`：仅借用最终拥有型结果，在原会话 poll 中执行，不接收 Session，不允许重入同会话。先记录终结状态再通知，观察器 panic 不重新执行操作。主要接口使用 Ticket；该观察器不是 I/O 线程回调。

## 7. 扫描和诊断

`RecordScanner::next_record(&mut self)` 返回 `Result<Option<ScannedRecord<S>>, ScanError>`，首版返回拥有型记录（地址、头信息、拥有键值或墓碑）；不让应用永久 pin 日志页。缓冲模式保留无预读、单页、双页。扫描不去重、不保证全局一致快照；读取单条可变记录仍必须遵守值访问同步协议。

扫描前验证范围，使用短期页租约和扫描租约协调段删除；并发逻辑截断使后续范围失效时返回 `RangeTruncated`，不能悄悄解释为正常结束。长期扫描不能无限隐藏回收阻塞，诊断报告其租约。

P6.3 已接入公开扫描驱动：Unbuffered/SinglePage/DoublePage 分别持有最多 1/2/3 个逻辑页槽，即当前页加 0/1/2 个预读页。`Config.scan` 默认最多 16 个扫描器、每次同步等待 30 秒；关闭中的在途读取仍占名额。墓碑和 invalid 记录返回拥有键及 None 值，invalid 键不可规范解码时报错。close 停止预读并排空已接受读取，Drop 交给引擎继续收尾。详见 [P6.3 交付记录](acceptance/P6.3物理扫描交付记录.md)。

Diagnostics 返回结构化的 `log_span_bytes`、地址边界、会话数、在途数、缓存和压缩状态、索引分布及统计快照。`log_span_bytes` 对应原 Size 的日志跨度；不命名为 len 以免被当作键数量。格式化输出由 examples/工具或 tracing 完成。

## 8. 设计中有意改变的接口形式

取消 DeepCopy/CallbackContext；转为拥有型请求与结果。取消 public hlog/hash_index 原始字段；转为诊断和受控扩展。Recover 改为返回就绪实例，维护 bool 改为启动结果加完成句柄。条件结果保持含义，错误补充副作用状态，方法名遵循 Rust 的 snake_case，主对象保留用户指定的 RasterKV。

这些调整保留第一阶段功能，减少误用方式；不承诺 C++ ABI、文件格式或调用语法兼容。

P1.3 的值编码辅助接口和尺寸规则见 [值编码契约](acceptance/P1.3值编码契约.md)。ValueCodec 与 PreparedValue 可独立使用；实际页许可和内建 ValueLayout 留到 P2.2。


## 在线索引扩容实施说明

Maintenance::grow_index 将当前桶数加倍，返回的票据通过维护 poll 或 Session::wait_maintenance 推进。参与会话须 refresh/poll 观察阶段；维护轮询不执行其他会话的业务回调。只有全部桶完成迁移并安全释放旧表后，IndexGrowthReport 才报告 old_buckets、new_buckets 和 generation。重复或冲突动作返回 Busy，丢弃票据不取消扩容。

扩容保持日志检查点版本和逻辑记录地址不变。恢复仍要求 Config.index.buckets 与所选索引材料一致，调用方应保存报告的 new_buckets；初始配置值不等于在线扩容后的当前容量。P6.1 的实现与证据见 [索引扩容交付记录](acceptance/P6.1索引扩容交付记录.md)。


## 读缓存实施说明

Config.cache.enabled 启用冷日志读缓存，capacity_bytes 限制缓存记录的计费内存。计费包含拥有型记录容量和固定结构，读者仍持有的已淘汰记录继续计费；它不是进程 RSS 上限。缓存不足或仲裁竞争只跳过可选安装，后续仍可从日志读取。

写入和删除更换索引头后旧缓存立即失效；检查点与扩容先规范化主日志地址，恢复总是创建空缓存。启用缓存不改变四操作结果与恢复语义，具体证据见 [读缓存交付记录](acceptance/P6.2读缓存交付记录.md)。

## P7.1 压缩实施契约

当前实现为 `Maintenance::compact(CompactionOptions) -> Result<MaintenanceTicket<CompactionReport>, Error>`，支持 ScanDedup 和 Lookup 单工作者，半开范围为当前 begin 至 until。当前 `workers=1`；shift_begin/checkpoint 的可选后续动作已接通，多工作者与自动调度在 P7.3 继续实施。维护等待、Session::poll 和 Maintenance::poll 都可以推进同一任务；票据超时不取消任务。

成功报告的 copied 包含迁移的最新墓碑；未请求后续动作时 gc/checkpoint 为 None，begin 与会话序号不变。失败报告是 `Error::CompactionFailed { until, copied, checkpoint, gc, cause }`：until 是请求边界，copied 是已发布迁移数，checkpoint/gc 保留已经完整成功的子步骤（错误中使用 Box），cause 保留子步骤原始错误及部分效果。普通失败排空后释放动作并保留已发生效果；恐慌失败关闭。压缩不是原子批处理，也不单独声明持久化成功。

ScanDedup 的不同键数与键字节预算分别为 `maintenance.max_compaction_keys`（默认 1,000,000）和 `maintenance.max_compaction_key_bytes`（默认 64 MiB）。预算不足时本次动作失败，不能未经声明切换算法。Lookup 不累积候选表。


## P7.2 已接通的工作段回收

`shift_begin(address)` 已返回真实维护票据。热边界非法直接拒绝；冷边界验证失败通过票据报告 `GcFailed`。逻辑 begin 发布后普通查询对旧键返回缺失，跨界物理扫描返回 RangeTruncated。GC 不搬迁被截断范围中的最新值，需要保留它们时先完成压缩。

`GcReport { begin, index_cleaned, deleted_segments, physical }` 分离逻辑截断、索引清理与物理结果。旧页或在途读取阻碍回收时返回 `DeferredByRuntime { begin, end }` 并结束动作；可在读者推进后用相同地址重试。普通失败为 `GcFailed { begin, index_cleaned, deleted_segments, cause }`，保留部分效果；显式重试接续关闭/删除/同步步骤，额外轮询不自动重试。

v1 检查点持有独立材料副本，工作段回收不会删除这些材料。GC 后旧自动索引配对失效，需要新 Index/Full 后再发起 Log 检查点。显式释放由以下独立 API 处理，验证证据见 [回收交付记录](acceptance/P7.2回收交付记录.md)。


## 检查点显式释放

`Maintenance::release_checkpoint(token)` 接受后返回 `MaintenanceTicket<CheckpointReleaseReport>`。调用表示放弃该 token 的恢复能力；它可以来自本次进程未曾登记的旧检查点。仍被已提交 Log 引用的 Index/Full 返回 `DeferredByRecoverySet { blockers, begin, end }`，其中 blockers 来自磁盘实际元数据；本次没有删除，动作已经结束。先释放引用方，再显式重试基准 token。v1 的材料为各 token 独占副本，不与工作段 GC 混用。

| 失效状态 | 含义 |
| --- | --- |
| NotAttempted | 本次未发出失效操作；不保证未知或以前已失效的 token 可恢复 |
| PossiblyRetired | 已接受失效操作或发现先前失效标识，但本次尚未确认目录同步；不能假设仍可恢复 |
| Retired | 已确认失效目录同步；后续材料删除仍可能失败 |

报告和 `Error::CheckpointReleaseFailed { token, retirement, confirmed_absent_materials, cause }` 均保留已知效果。`confirmed_absent_materials` 是本次经目录同步确认不存在的材料数，包含此前已经删除、此次显式重试重新确认的材料；不能用作新增删除数或释放字节数。Completed 表示清单材料均处理完成，owner、manifest、commit.released 及 token 目录继续保留以防 token 复用并支持跨重启重试。

实例内动作冲突在接受前返回 Busy；目录锁与其他实例发布/恢复发生竞争时，通过已接受票据报告 cause=Busy，释放本次动作，需显式重试。未知目录、链接、损坏或缺失的清单、断裂引用以及超预算均保守失败，没有按运行期目录缺项授权删除。正常失败收尾后可继续业务、检查点或再次释放；身份错误、panic 或未确认资源进入失败关闭。

`maintenance.max_checkpoint_tokens` 默认 4096；`max_checkpoint_catalog_bytes` 默认 64 MiB，必须非零。后者限制整次目录名称、commit 和 manifest 字节总量；达到预算时整体拒绝，不使用不完整目录。已释放 token 的必要元数据也计入预算；应按实际保留历史调整限制，目前不自动清理这些元数据。


## P7.3 已接通的压缩后续动作

设置 checkpoint=true 时，复制动作成功结束后发起 Full 检查点；设置 shift_begin=true 时，在可选检查点成功之后发起 GC。两者均启用的顺序与固定上游一致：复制 → 完整检查点 → 逻辑截断。报告中的检查点描述截断前的恢复状态，而 gc 描述随后推进的 begin。复制或检查点失败时不继续截断。

每个步骤分别争取并释放全局动作，同一张 CompactionReport 票据在最后一步结束后终结。动作之间允许其他维护进入，普通 Busy 稍后重试；已接受子步骤失败不自动重放。GC 的 DeferredByRuntime 是该子步骤的终结报告，复合任务不无限等待读者释放。检查点材料继续默认保留，压缩不会隐式调用 release_checkpoint。

维护等待超时保留原票据；无活跃会话时 Maintenance::poll 也可推进后续步骤。有活跃会话时仍须各自刷新检查点屏障。动作间隙仍计为未完成复合任务，shutdown 返回 Busy，不能越过间隙提前关闭。自动维护的排空、等待和停止将在 P7.3 后续接通。

普通失败先排空对应子任务并保留原始错误。例如检查点已完成而 GC 删除失败，CompactionFailed.checkpoint 保留可恢复 token，cause 为带实际 begin 和删除计数的 GcFailed。业务 panic 导致失败关闭时也先收取子步骤的部分效果。进度与验证边界见 [P7.3 交付记录](acceptance/P7.3自动维护交付记录.md)。
