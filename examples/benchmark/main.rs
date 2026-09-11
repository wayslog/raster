//! 四操作与恢复的固定矩阵；正确性、时间和实际传输字节分别记录。
mod meter;
#[path = "../../tests/support/model.rs"]
#[allow(
    dead_code,
    reason = "基准输入由确定值模型预计算；非确定盲删观察另有专门轨迹验收"
)]
mod model;
mod operation;
mod scenario;
#[allow(
    dead_code,
    reason = "与轨迹验收复用完整文本格式；本程序只生成并核对固定输入"
)]
#[path = "../../tests/support/trace.rs"]
mod trace;

use meter::Meter;
use model::ResultValue;
use operation::{Fixed, Kind, Request, Schema, Variable};
use raster::{
    RasterKV, Session, Submission,
    api::{
        Outcome,
        completion::AbortReason,
        maintenance::{CheckpointKind, CheckpointReport, RecoverySet},
        operation::*,
    },
    config::Config,
    schema::builtin::{SchemaPair, U64Key},
    types::*,
};
use scenario::{Case, KEYS, OPERATIONS, PAGE_BYTES, Prepared};
use std::{
    fs,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Barrier,
    time::{Duration, Instant},
};
use trace::{Operation, Step, Value};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Debug)]
struct RequestFailure {
    session: u64,
    serial: u64,
    operation: &'static str,
    cause: OperationError,
}
impl std::fmt::Display for RequestFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "基准会话 {} 序号 {} 的{}失败：{}",
            self.session, self.serial, self.operation, self.cause
        )
    }
}
impl std::error::Error for RequestFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(300))
}
fn request<K: Kind>(step: &Step) -> Result<Request<K>> {
    let key = u64::from_le_bytes(
        step.key
            .as_slice()
            .try_into()
            .map_err(|_| "基准键必须为八字节")?,
    );
    let input = match &step.operation {
        Operation::Upsert(v) | Operation::Rmw { operand: v, .. } => v.clone(),
        _ => Value::Number(0),
    };
    Ok(Request::new(key, input))
}
fn execute<K: Kind>(
    session: &mut Session<Schema<K>>,
    step: &Step,
    mut request: Request<K>,
    end: Deadline,
) -> Result<(ResultValue, bool, u64)> {
    let mut retries = 0;
    loop {
        if end.expired() {
            return Err(Error::DeadlineExceeded.into());
        }
        let submitted = match step.operation {
            Operation::Read { abort_if_tombstone } => session.read(
                Serial(step.serial),
                request,
                ReadOptions { abort_if_tombstone },
            ),
            Operation::Upsert(_) => session.upsert(Serial(step.serial), request),
            Operation::Rmw {
                create_if_missing, ..
            } => session.rmw(
                Serial(step.serial),
                request,
                RmwOptions { create_if_missing },
            ),
            Operation::Delete { force_tombstone } => session.delete(
                Serial(step.serial),
                request,
                DeleteOptions { force_tombstone },
            ),
        };
        let (result, pending) = match submitted {
            Err(rejected) if matches!(rejected.reason, Error::Busy) => {
                request = rejected.request;
                retries += 1;
                session.poll(PollBudget::default())?;
                std::thread::yield_now();
                continue;
            }
            Err(rejected) => return Err(rejected.reason.into()),
            Ok(Submission::Ready(result)) => (result, false),
            Ok(Submission::Pending(mut ticket)) => (session.wait(&mut ticket, end)?, true),
        };
        let result = result.map_err(|cause| RequestFailure {
            session: step.session,
            serial: step.serial,
            operation: match step.operation {
                Operation::Read { .. } => "读取",
                Operation::Upsert(_) => "写入",
                Operation::Rmw { .. } => "读改写",
                Operation::Delete { .. } => "删除",
            },
            cause,
        })?;
        let value = match result {
            Outcome::Success(value) => value,
            Outcome::NotFound => ResultValue::NotFound,
            Outcome::Aborted(AbortReason::Tombstone) => ResultValue::Tombstone,
            Outcome::Aborted(_) => return Err("基准收到非预期条件中止".into()),
        };
        return Ok((value, pending, retries));
    }
}
fn check(actual: &ResultValue, expected: &ResultValue, step: &Step) -> Result<()> {
    if !scenario::matches_result(step, actual, expected) {
        return Err(format!(
            "基准会话 {} 序号 {}：实际 {actual:?}，预期 {expected:?}",
            step.session, step.serial
        )
        .into());
    }
    Ok(())
}
fn build<K: Kind>(config: Config, meter: &Meter) -> raster::api::Builder<Schema<K>> {
    RasterKV::builder(SchemaPair::new(U64Key, K::layout()))
        .config(config)
        .device(Box::new(meter.clone()))
}
fn checkpoint<K: Kind>(store: &RasterKV<Schema<K>>) -> Result<CheckpointReport> {
    let ticket = store.maintenance().checkpoint(CheckpointKind::Full)?;
    let end = deadline();
    loop {
        if end.expired() {
            return Err(Error::DeadlineExceeded.into());
        }
        store.maintenance().poll(PollBudget::default())?;
        if let Some(report) = ticket.try_report()? {
            return report
                .as_ref()
                .as_ref()
                .cloned()
                .map_err(|error| format!("基准检查点失败：{error}").into());
        }
        std::thread::yield_now();
    }
}
fn shutdown<K: Kind>(store: &RasterKV<Schema<K>>) -> Result<()> {
    let end = deadline();
    loop {
        match store.shutdown(end) {
            Ok(report) if report.device_drained => return Ok(()),
            Ok(_) => return Err("基准关闭没有排空设备".into()),
            Err(Error::Busy) if !end.expired() => {
                store.maintenance().poll(PollBudget::default())?;
                std::thread::yield_now();
            }
            Err(error) => return Err(error.into()),
        }
    }
}
fn rss() -> Result<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()?;
    if !output.status.success() {
        return Err("无法采样本进程 RSS".into());
    }
    Ok(String::from_utf8(output.stdout)?
        .trim()
        .parse::<u64>()?
        .checked_mul(1024)
        .ok_or(Error::CapacityExceeded)?)
}
struct WorkerResult {
    id: SessionId,
    serial: Serial,
    finished: Instant,
    latency: Vec<u128>,
    by_kind: [Vec<u128>; 4],
    pending: [usize; 4],
    retries: u64,
    success: usize,
    missing: usize,
    tombstones: usize,
    application_bytes: u64,
    verify: Vec<(Step, ResultValue)>,
}
fn worker<K: Kind>(
    store: &RasterKV<Schema<K>>,
    prepared: Prepared,
    ready: &Barrier,
    go: &Barrier,
) -> Result<WorkerResult> {
    let Prepared {
        fill,
        work,
        verify,
        application_bytes,
    } = prepared;
    let initialized = (|| -> Result<_> {
        let mut session = store.start_session(Default::default())?;
        let end = deadline();
        for (step, expected) in fill {
            let (actual, _, _) = execute::<K>(&mut session, &step, request::<K>(&step)?, end)?;
            check(&actual, &expected, &step)?;
        }
        let work = work
            .into_iter()
            .map(|(step, expected)| Ok((request::<K>(&step)?, step, expected)))
            .collect::<Result<Vec<_>>>()?;
        let result = WorkerResult {
            id: session.id(),
            serial: Serial(0),
            finished: Instant::now(),
            latency: Vec::with_capacity(work.len()),
            by_kind: std::array::from_fn(|_| Vec::with_capacity(work.len() / 4)),
            pending: [0; 4],
            retries: 0,
            success: 0,
            missing: 0,
            tombstones: 0,
            application_bytes,
            verify,
        };
        Ok((session, work, result))
    })();
    // 即使预装失败也到达两个屏障，让其他线程退出并由主线程保留失败材料。
    ready.wait();
    go.wait();
    let (mut session, work, mut result) = initialized?;
    let end = deadline();
    for (request, step, expected) in work {
        let kind = match step.operation {
            Operation::Read { .. } => 0,
            Operation::Upsert(_) => 1,
            Operation::Rmw { .. } => 2,
            Operation::Delete { .. } => 3,
        };
        let start = Instant::now();
        let (actual, pending, retries) = execute::<K>(&mut session, &step, request, end)?;
        let elapsed = start.elapsed().as_nanos();
        check(&actual, &expected, &step)?;
        result.latency.push(elapsed);
        result.by_kind[kind].push(elapsed);
        result.pending[kind] += usize::from(pending);
        result.retries += retries;
        match actual {
            ResultValue::NotFound => result.missing += 1,
            ResultValue::Tombstone => result.tombstones += 1,
            _ => result.success += 1,
        };
        result.serial = Serial(step.serial);
    }
    result.finished = Instant::now();
    session.close(deadline())?;
    Ok(result)
}
fn percentile(values: &mut [u128], percent: usize) -> u128 {
    values.sort_unstable();
    values[(values.len() * percent / 100).min(values.len() - 1)]
}
fn run<K: Kind>(case: Case, output: &Path) -> Result<Vec<String>> {
    let prepared: Vec<_> = (0..case.threads)
        .map(|worker| scenario::prepare(case, worker))
        .collect();
    let trace = scenario::trace(&prepared);
    let text = trace.encode();
    if trace::Trace::decode(&text)? != trace {
        return Err("基准轨迹往返不一致".into());
    }
    let path = output.join(format!("{}.trace", case.input_id()));
    if path.exists() {
        if fs::read_to_string(&path)? != text {
            return Err("相同场景的轨迹发生变化".into());
        }
    } else {
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?
            .write_all(text.as_bytes())?;
    }
    drop(trace);
    drop(text);
    let data = output.join(case.id());
    fs::create_dir(&data)?;
    let mut config = Config::default();
    config.storage.root = data.clone();
    config.storage.segment_bytes = 1024 * 1024;
    config.index.buckets = 2048;
    config.log.page_bytes = PAGE_BYTES;
    config.log.memory_pages = if case.disk { 4 } else { 4096 };
    config.log.mutable_fraction = 0.5;
    let budget = (config.log.page_bytes * config.log.memory_pages) as u64;
    let meter = Meter::default();
    let store = build::<K>(config.clone(), &meter).create()?;
    let ready = Barrier::new(case.threads + 1);
    let go = Barrier::new(case.threads + 1);
    let (baseline, start, results) = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for prepared in prepared {
            workers.push(scope.spawn(|| worker::<K>(&store, prepared, &ready, &go)));
        }
        ready.wait();
        let baseline = (|| -> Result<_> { Ok((store.diagnostics()?, meter.snapshot(), rss()?)) })();
        let start = Instant::now();
        go.wait();
        let results = workers
            .into_iter()
            .map(|worker| {
                worker
                    .join()
                    .map_err(|_| "基准工作线程恐慌".into())
                    .and_then(|result| result)
            })
            .collect::<Result<Vec<_>>>();
        (baseline, start, results)
    });
    let (initial, initial_io, initial_rss) = baseline?;
    let results = results?;
    if case.disk && initial.log_span_bytes <= budget {
        return Err("预装数据没有超过配置的驻留窗口".into());
    }
    let elapsed = results
        .iter()
        .map(|r| r.finished.duration_since(start))
        .max()
        .ok_or("基准缺少工作线程")?;
    let after = store.diagnostics()?;
    let after_io = meter.snapshot();
    let after_rss = rss()?;
    let pending: [usize; 4] = std::array::from_fn(|i| results.iter().map(|r| r.pending[i]).sum());
    if !case.disk
        && (pending.iter().sum::<usize>() != 0
            || after_io.read != 0
            || after_io.written != 0
            || after.log_span_bytes >= budget)
    {
        return Err("纯内存组出现实际 I/O、挂起或超过窗口".into());
    }
    if case.disk
        && (pending.iter().sum::<usize>() == 0 || after_io.read == 0 || after_io.written == 0)
    {
        return Err("超内存组没有实际挂起及读写传输".into());
    }
    let checkpoint_start = Instant::now();
    let checkpoint = checkpoint::<K>(&store)?;
    let checkpoint_ns = checkpoint_start.elapsed().as_nanos();
    let checkpoint_rss = rss()?;
    if checkpoint.sessions.len() != results.len() {
        return Err("检查点没有保留全部工作会话".into());
    }
    for worker in &results {
        if !checkpoint
            .sessions
            .iter()
            .any(|p| p.session == worker.id && p.serial == worker.serial)
        {
            return Err("检查点会话切分不匹配".into());
        }
    }
    let set = RecoverySet {
        store: store.id(),
        index: checkpoint.token,
        log: checkpoint.token,
    };
    shutdown::<K>(&store)?;
    drop(store);
    let durable_io = meter.snapshot();
    let recovery_meter = Meter::default();
    let recovery_start = Instant::now();
    let (store, recovery) = build::<K>(config, &recovery_meter).recover(set)?;
    let recovery_ns = recovery_start.elapsed().as_nanos();
    let recovery_io = recovery_meter.snapshot();
    let recovery_rss = rss()?;
    if recovery.sessions.len() != results.len() {
        return Err("恢复报告缺少会话".into());
    }
    for worker in &results {
        let resumed = store.continue_session(worker.id)?;
        if resumed.progress.serial != worker.serial {
            return Err("续接会话进度不匹配".into());
        }
        let mut session = resumed.session;
        for (step, expected) in &worker.verify {
            let (actual, _, _) = execute::<K>(&mut session, step, request::<K>(step)?, deadline())?;
            check(&actual, expected, step)?;
        }
        session.close(deadline())?;
    }
    shutdown::<K>(&store)?;
    drop(store);
    fs::remove_dir_all(&data)?;
    let mut latencies: Vec<_> = results
        .iter()
        .flat_map(|r| r.latency.iter().copied())
        .collect();
    if latencies.len() != OPERATIONS {
        return Err("基准执行操作数不完整".into());
    }
    let applications: u64 = results.iter().map(|r| r.application_bytes).sum();
    let mut row = vec![
        if case.variable {
            "变长字节"
        } else {
            "原子u64"
        }
        .into(),
        if case.hot {
            "每线程热点"
        } else {
            "均匀"
        }
        .into(),
        case.threads.to_string(),
        if case.disk { "超内存" } else { "纯内存" }.into(),
        case.round.to_string(),
        scenario::SEED.to_string(),
        KEYS.to_string(),
        OPERATIONS.to_string(),
        format!("{:.3}", OPERATIONS as f64 / elapsed.as_secs_f64()),
    ];
    for p in [50, 95, 99] {
        row.push(percentile(&mut latencies, p).to_string());
    }
    for (kind, pending) in pending.iter().enumerate() {
        let mut values: Vec<_> = results
            .iter()
            .flat_map(|r| r.by_kind[kind].iter().copied())
            .collect();
        if values.len() != OPERATIONS / 4 {
            return Err("四操作比例发生变化".into());
        }
        row.push(values.len().to_string());
        row.push(percentile(&mut values, 99).to_string());
        row.push(pending.to_string());
    }
    row.extend([
        results.iter().map(|r| r.retries).sum::<u64>().to_string(),
        results.iter().map(|r| r.success).sum::<usize>().to_string(),
        results.iter().map(|r| r.missing).sum::<usize>().to_string(),
        results
            .iter()
            .map(|r| r.tombstones)
            .sum::<usize>()
            .to_string(),
        budget.to_string(),
        initial.log_span_bytes.to_string(),
        after.log_span_bytes.to_string(),
        after.log_allocated_bytes.to_string(),
        applications.to_string(),
    ]);
    for counts in [initial_io, after_io, durable_io, recovery_io] {
        row.push(counts.read.to_string());
        row.push(counts.written.to_string());
    }
    row.push(format!(
        "{:.6}",
        durable_io.written as f64 / applications as f64
    ));
    for n in [initial_rss, after_rss, checkpoint_rss, recovery_rss] {
        row.push(n.to_string());
    }
    row.extend([
        checkpoint_ns.to_string(),
        recovery_ns.to_string(),
        case.input_id(),
    ]);
    eprintln!(
        "基准 {} 通过：{OPERATIONS} 步四操作、{} 个会话恢复、{KEYS} 个键与墓碑校验",
        case.id(),
        case.threads
    );
    Ok(row)
}
fn main() -> Result<()> {
    if std::env::args_os().len() != 1 {
        return Err("基准不接受位置参数".into());
    }
    let output = std::env::var_os("RASTER_BENCH_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| "target/p9-benchmark".into());
    fs::create_dir(&output)?;
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join("matrix.csv"))?;
    let mut csv = BufWriter::new(file);
    let mut columns: Vec<String> = [
        "布局",
        "分布",
        "线程",
        "存储",
        "轮次",
        "种子",
        "键数",
        "操作数",
        "每秒操作",
        "P50纳秒",
        "P95纳秒",
        "P99纳秒",
    ]
    .map(String::from)
    .to_vec();
    for kind in ["读取", "写入", "RMW", "删除"] {
        for metric in ["操作数", "P99纳秒", "挂起数"] {
            columns.push(format!("{kind}{metric}"));
        }
    }
    columns.extend(
        [
            "拒绝重试",
            "成功",
            "缺失",
            "墓碑",
            "窗口字节",
            "预装日志跨度",
            "业务后日志跨度",
            "业务后驻留页字节",
            "应用写入字节",
            "预装累计读字节",
            "预装累计写字节",
            "业务后累计读字节",
            "业务后累计写字节",
            "持久化累计读字节",
            "持久化累计写字节",
            "恢复读字节",
            "恢复写字节",
            "写放大含预装检查点",
            "预装RSS字节",
            "业务后RSS字节",
            "检查点后RSS字节",
            "恢复后RSS字节",
            "检查点纳秒",
            "恢复纳秒",
            "输入标识",
        ]
        .map(String::from),
    );
    writeln!(csv, "{}", columns.join(","))?;
    csv.flush()?;
    let selected = std::env::var("RASTER_BENCH_CASE").ok();
    let mut count = 0;
    for variable in [false, true] {
        for hot in [false, true] {
            for threads in [1, 4] {
                for disk in [false, true] {
                    for round in 1..=3 {
                        let case = Case {
                            variable,
                            hot,
                            threads,
                            disk,
                            round,
                        };
                        if selected.as_ref().is_some_and(|name| *name != case.id()) {
                            continue;
                        }
                        let row = if variable {
                            run::<Variable>(case, &output)?
                        } else {
                            run::<Fixed>(case, &output)?
                        };
                        if row.len() != columns.len() {
                            return Err("基准 CSV 列数不匹配".into());
                        }
                        writeln!(csv, "{}", row.join(","))?;
                        csv.flush()?;
                        count += 1;
                    }
                }
            }
        }
    }
    if count != if selected.is_some() { 1 } else { 48 } {
        return Err("基准场景选择为空或矩阵不完整".into());
    }
    println!("性能矩阵业务校验通过：{count} 组；数值不代表原性能回退已解决。");
    Ok(())
}
