//! 仅用公开接口执行超内存、变长值、检查点、恢复续跑、压缩回收和诊断。
mod control;
mod operation;
use control::{Result, deadline, report, shutdown, success, take};
use operation::{Request, Schema};
use raster::{
    RasterKV, Session,
    api::{Outcome, maintenance::*, operation::*, scan::*},
    config::Config,
    device::thread_pool::ThreadPoolDeviceFactory,
    schema::builtin::{ByteKey, ByteValueCodec, SchemaPair, SerializedValue},
    types::*,
};
use std::{collections::BTreeMap, path::PathBuf};

fn builder(config: Config) -> raster::Builder<Schema> {
    RasterKV::builder(SchemaPair::new(
        ByteKey,
        SerializedValue::new(ByteValueCodec),
    ))
    .config(config)
    .device(Box::new(ThreadPoolDeviceFactory {
        workers: 2,
        queue_capacity: 128,
    }))
}
fn next(serial: &mut u64) -> Serial {
    let value = Serial(*serial);
    *serial += 1;
    value
}
fn key(number: u8) -> Vec<u8> {
    if number == 0 {
        Vec::new()
    } else {
        vec![number, 255]
    }
}
type Expected = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

fn fill(session: &mut Session<Schema>, serial: &mut u64) -> Result<Expected> {
    let mut expected = Expected::new();
    for generation in 0..2u8 {
        for number in 0..40u8 {
            let key = key(number);
            let value = if number == 0 {
                Vec::new()
            } else {
                vec![number + generation; 1500 + usize::from(number % 4) * 67]
            };
            let submitted = session
                .upsert(
                    next(serial),
                    Request {
                        key: key.clone(),
                        value: value.clone(),
                    },
                )
                .map_err(|rejected| rejected.reason)?;
            success(take(session, submitted)?)?;
            expected.insert(key, Some(value));
        }
    }
    for number in 1..6 {
        let key = key(number);
        let submitted = session
            .delete(
                next(serial),
                Request::key(key.clone()),
                DeleteOptions {
                    force_tombstone: true,
                },
            )
            .map_err(|rejected| rejected.reason)?;
        success(take(session, submitted)?)?;
        expected.insert(key, None);
    }
    let submitted = session
        .rmw(
            next(serial),
            Request {
                key: Vec::new(),
                value: b"append".to_vec(),
            },
            RmwOptions::default(),
        )
        .map_err(|rejected| rejected.reason)?;
    let value = success(take(session, submitted)?)?;
    if value != b"append" {
        return Err("变长追加结果不匹配".into());
    }
    expected.insert(Vec::new(), Some(value));
    Ok(expected)
}
fn verify(session: &mut Session<Schema>, serial: &mut u64, expected: &Expected) -> Result<()> {
    for (key, value) in expected {
        let submitted = session
            .read(
                next(serial),
                Request::key(key.clone()),
                ReadOptions::default(),
            )
            .map_err(|rejected| rejected.reason)?;
        match (take(session, submitted)?, value) {
            (Outcome::Success(actual), Some(expected)) if &actual == expected => {}
            (Outcome::NotFound, None) => {}
            _ => return Err("恢复或维护后的拥有型值不匹配".into()),
        }
    }
    Ok(())
}
fn scan(store: &RasterKV<Schema>, checkpoint: &CheckpointReport) -> Result<()> {
    for buffering in [
        Buffering::Unbuffered,
        Buffering::SinglePage,
        Buffering::DoublePage,
    ] {
        let mut scanner = store.scan(ScanOptions {
            begin: checkpoint.begin,
            end: checkpoint.end,
            buffering,
        })?;
        let mut records = 0;
        let mut tombstones = 0;
        while let Some(record) = scanner.next_record()? {
            records += 1;
            tombstones += usize::from(record.tombstone);
        }
        scanner.close()?;
        if records == 0 || tombstones < 5 {
            return Err("物理扫描没有覆盖记录与墓碑".into());
        }
        println!("物理扫描 {buffering:?}：{records} 条记录，{tombstones} 条墓碑；不代表有效键数量");
    }
    Ok(())
}
fn show(store: &RasterKV<Schema>, stage: &str) -> Result<()> {
    let d = store.diagnostics()?;
    println!(
        "{stage}：日志 [{}..{})，跨度 {} 字节；内存日志 {} 字节；缓存计费 {} 字节；桶数 {}；会话 {}；在途 {}",
        d.begin.0,
        d.tail.0,
        d.log_span_bytes,
        d.log_allocated_bytes,
        d.cached_bytes,
        d.bucket_distribution.len(),
        d.active_sessions,
        d.active_requests
    );
    Ok(())
}
fn first(
    store: &RasterKV<Schema>,
    config: &mut Config,
) -> Result<(RecoverySet, SessionId, Serial, Expected)> {
    let mut session = store.start_session(Default::default())?;
    let mut serial = 0;
    let expected = fill(&mut session, &mut serial)?;
    if store.diagnostics()?.log_span_bytes
        <= (config.log.page_bytes * config.log.memory_pages) as u64
    {
        return Err("数据量没有超过内存日志预算".into());
    }
    for _ in 0..2 {
        let submitted = session
            .read(
                next(&mut serial),
                Request::key(key(6)),
                ReadOptions::default(),
            )
            .map_err(|rejected| rejected.reason)?;
        let actual = success(take(&mut session, submitted)?)?;
        if expected.get(&key(6)) != Some(&Some(actual)) {
            return Err("冷读或缓存命中结果不匹配".into());
        }
    }
    let statistics = store.statistics();
    if statistics.reads.io_completions == 0 || statistics.cache.hits == 0 {
        return Err("没有实际发生磁盘读与缓存命中".into());
    }
    println!(
        "冷读与缓存命中通过：实际读取 I/O 完成 {}，缓存命中 {}",
        statistics.reads.io_completions, statistics.cache.hits
    );
    let grown = report(&mut session, &store.maintenance().grow_index()?)?;
    config.index.buckets = grown.new_buckets;
    let index = report(
        &mut session,
        &store.maintenance().checkpoint(CheckpointKind::Index)?,
    )?;
    if !index.sessions.is_empty() {
        return Err("仅索引检查点错误地声明了会话进度".into());
    }
    let log = report(
        &mut session,
        &store.maintenance().checkpoint(CheckpointKind::Log)?,
    )?;
    scan(store, &log)?;
    show(store, "首次检查点完成")?;
    let id = session.id();
    let durable = log
        .sessions
        .iter()
        .find(|cut| cut.session == id)
        .ok_or("日志检查点缺少会话进度")?
        .serial;
    if durable != Serial(serial - 1) {
        return Err("会话检查点序号不匹配".into());
    }
    session.close(deadline())?;
    Ok((
        RecoverySet {
            store: store.id(),
            index: index.token,
            log: log.token,
        },
        id,
        durable,
        expected,
    ))
}
fn second(
    store: &RasterKV<Schema>,
    old: &RecoverySet,
    id: SessionId,
    durable: Serial,
    expected: &mut Expected,
) -> Result<(RecoverySet, Serial)> {
    let resumed = store.continue_session(id)?;
    if resumed.progress.serial != durable {
        return Err("恢复续会话进度不匹配".into());
    }
    let mut session = resumed.session;
    let mut serial = durable.0 + 1;
    verify(&mut session, &mut serial, expected)?;
    let submitted = session
        .rmw(
            next(&mut serial),
            Request {
                key: Vec::new(),
                value: b"-resumed".to_vec(),
            },
            RmwOptions::default(),
        )
        .map_err(|rejected| rejected.reason)?;
    let value = success(take(&mut session, submitted)?)?;
    if value != b"append-resumed" {
        return Err("恢复后追加没有保留旧值".into());
    }
    expected.insert(Vec::new(), Some(value));
    let before = report(
        &mut session,
        &store.maintenance().checkpoint(CheckpointKind::Full)?,
    )?;
    let copied = report(
        &mut session,
        &store.maintenance().compact(CompactionOptions {
            algorithm: CompactionAlgorithm::ScanDedup,
            until: before.end,
            workers: 2,
            shift_begin: false,
            checkpoint: false,
        })?,
    )?;
    if copied.copied == 0 || copied.gc.is_some() || copied.checkpoint.is_some() {
        return Err("扫描去重压缩结果不符合选项".into());
    }
    let middle = report(
        &mut session,
        &store.maintenance().checkpoint(CheckpointKind::Full)?,
    )?;
    let compacted = report(
        &mut session,
        &store.maintenance().compact(CompactionOptions {
            algorithm: CompactionAlgorithm::Lookup,
            until: middle.end,
            workers: 2,
            shift_begin: true,
            checkpoint: true,
        })?,
    )?;
    let copy_checkpoint = compacted
        .checkpoint
        .as_ref()
        .ok_or("查索引压缩缺少检查点报告")?;
    let gc = compacted.gc.as_ref().ok_or("查索引压缩缺少逻辑截断报告")?;
    if compacted.copied == 0 || gc.begin != middle.end {
        return Err("压缩迁移或逻辑截断未完成".into());
    }
    let gc = report(&mut session, &store.maintenance().shift_begin(gc.begin)?)?;
    if !matches!(gc.physical, PhysicalReclamation::Completed) {
        return Err("示例的工作段回收仍被保留约束延后".into());
    }
    verify(&mut session, &mut serial, expected)?;
    let final_checkpoint = report(
        &mut session,
        &store.maintenance().checkpoint(CheckpointKind::Full)?,
    )?;
    // 先释放引用索引的旧 Log，随后才可释放其索引；新恢复集始终保留。
    for token in [
        old.log,
        old.index,
        before.token,
        middle.token,
        copy_checkpoint.token,
    ] {
        let released = report(
            &mut session,
            &store.maintenance().release_checkpoint(token)?,
        )?;
        if released.retirement != CheckpointRetirement::Retired
            || !matches!(released.physical, PhysicalReclamation::Completed)
        {
            return Err("旧检查点未完成显式释放".into());
        }
    }
    store.maintenance().stop_auto_compaction()?;
    let auto = session.wait_auto_compaction(deadline())?;
    if !auto.is_quiescent() {
        return Err("自动维护尚未空闲".into());
    }
    show(store, "压缩与旧检查点释放完成")?;
    store.write_statistics(&mut std::io::stdout().lock())?;
    println!();
    session.close(deadline())?;
    Ok((
        RecoverySet {
            store: store.id(),
            index: final_checkpoint.token,
            log: final_checkpoint.token,
        },
        Serial(serial - 1),
    ))
}
fn closed<T>(store: RasterKV<Schema>, result: Result<T>) -> Result<T> {
    let cleanup = shutdown(&store);
    match (result, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(format!("流程失败：{error}；收尾失败：{cleanup}").into()),
    }
}
fn run(root: PathBuf) -> Result<()> {
    #[cfg(feature = "config-toml")]
    let mut config = Config::from_toml_str(include_str!("../../docs/examples/raster.toml"))?;
    #[cfg(not(feature = "config-toml"))]
    let mut config = Config::default();
    config.storage.root = root;
    config.storage.segment_bytes = 16384;
    config.storage.pre_allocate_log = true;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    config.index.buckets = 32;
    config.cache.enabled = true;
    config.cache.capacity_bytes = 32768;
    config.cache.pre_allocate = true;
    config.statistics.enabled = true;
    let store = builder(config.clone()).create()?;
    let result = first(&store, &mut config);
    let (old, id, durable, mut expected) = closed(store, result)?;
    let (store, recovery) = builder(config.clone()).recover(old.clone())?;
    if recovery.set.store != old.store {
        return closed(store, Err("持久存储身份变化".into()));
    }
    let result = second(&store, &old, id, durable, &mut expected);
    let (set, durable) = closed(store, result)?;
    let (store, _) = builder(config).recover(set)?;
    let result = (|| {
        let resumed = store.continue_session(id)?;
        if resumed.progress.serial != durable {
            return Err("最终恢复的序号不匹配".into());
        }
        let mut session = resumed.session;
        verify(&mut session, &mut (durable.0 + 1), &expected)?;
        session.close(deadline())?;
        show(&store, "最终恢复校验完成")
    })();
    closed(store, result)?;
    println!(
        "磁盘生命周期通过：40 个键的变长值与墓碑，三种检查点，两次恢复续跑，两种压缩，回收与旧检查点释放，真实诊断统计。"
    );
    Ok(())
}
fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let root = args.next().map(PathBuf::from);
    if args.next().is_some() {
        return Err("用法：disk_lifecycle [全新数据目录]".into());
    }
    let temporary = root.is_none();
    let root = match root {
        Some(root) => root,
        None => {
            std::env::temp_dir().join(format!("raster-lifecycle-{:x?}", StoreId::generate()?.0))
        }
    };
    if root.exists() {
        return Err("示例要求不存在的新目录；不会覆盖已有数据".into());
    }
    std::fs::create_dir(&root)?;
    let result = run(root.clone());
    if result.is_ok() && temporary {
        std::fs::remove_dir_all(root)?;
    }
    result
}
