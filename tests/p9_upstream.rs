//! 上游对照的 Rust 执行端：相同轨迹逐操作验证模型，再输出真实返回值。
#[path = "support/actor.rs"]
mod actor;
#[path = "support/replay.rs"]
mod replay;
#[allow(dead_code)]
mod support;
use raster::{
    RasterKV,
    api::{
        maintenance::{CheckpointKind, CheckpointReport, RecoverySet},
        session::SessionOptions,
    },
    config::Config,
    device::null::NullDeviceFactory,
    schema::builtin::{ByteKey, SchemaPair, SerializedValue},
    types::{Deadline, PollBudget},
};
use std::{
    collections::BTreeMap,
    fmt::Write,
    time::{Duration, Instant},
};
use support::{
    model::{Model, ResultValue, Submission},
    trace::{Trace, Value},
};
fn encode(result: &Submission) -> String {
    match result {
        Submission::Rejected(_) => "rejected".into(),
        Submission::Accepted(result) => match result {
            ResultValue::Written => "written".into(),
            ResultValue::Deleted => "deleted".into(),
            ResultValue::NotFound => "missing".into(),
            ResultValue::Tombstone => "tombstone".into(),
            ResultValue::TypeMismatch => "type-mismatch".into(),
            ResultValue::Value(Value::Number(n)) => format!("u {n}"),
            ResultValue::Value(Value::Bytes(bytes)) => {
                let mut text = String::from("b ");
                if bytes.is_empty() {
                    text.push('-');
                }
                for byte in bytes {
                    write!(text, "{byte:02x}").unwrap();
                }
                text
            }
        },
    }
}
type Sessions = BTreeMap<u64, actor::Actor<raster::Session<replay::Schema>>>;
fn deadline() -> Deadline {
    let seconds =
        std::env::var("RASTER_UPSTREAM_TIMEOUT").map_or(60, |value| value.parse::<u64>().unwrap());
    assert!((1..=600).contains(&seconds), "轨迹等待预算越界");
    Deadline(Instant::now() + Duration::from_secs(seconds))
}
fn builder(config: Config, native: bool) -> raster::Builder<replay::Schema> {
    let factory: Box<dyn raster::device::DeviceFactory> = if native {
        Box::new(raster::device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 128,
        })
    } else {
        Box::new(NullDeviceFactory)
    };
    RasterKV::builder(SchemaPair::new(
        ByteKey,
        SerializedValue::new(replay::Codec),
    ))
    .config(config)
    .device(factory)
}
fn checkpoint(
    store: &RasterKV<replay::Schema>,
    sessions: &Sessions,
    kind: CheckpointKind,
) -> CheckpointReport {
    let ticket = store.maintenance().checkpoint(kind).unwrap();
    let end = deadline();
    loop {
        for session in sessions.values() {
            session.call(|session| session.poll(PollBudget::default()).unwrap());
        }
        store.maintenance().poll(PollBudget::default()).unwrap();
        if let Some(report) = ticket.try_report().unwrap() {
            return report.as_ref().as_ref().unwrap().clone();
        }
        assert!(!end.expired(), "轨迹边界检查点超时");
        std::thread::yield_now();
    }
}
fn recover_boundary(
    store: RasterKV<replay::Schema>,
    sessions: &mut Sessions,
    config: Config,
    pair: bool,
) -> RasterKV<replay::Schema> {
    let index = checkpoint(
        &store,
        sessions,
        if pair {
            CheckpointKind::Index
        } else {
            CheckpointKind::Full
        },
    );
    let log = if pair {
        checkpoint(&store, sessions, CheckpointKind::Log)
    } else {
        index.clone()
    };
    let identities: Vec<_> = sessions
        .iter()
        .map(|(&logical, session)| {
            let (id, serial) =
                session.call(|session| (session.id(), session.last_accepted().unwrap()));
            assert!(
                log.sessions
                    .iter()
                    .any(|p| p.session == id && p.serial == serial)
            );
            session.call(|session| session.close(deadline()).unwrap());
            (logical, id, serial)
        })
        .collect();
    sessions.clear();
    let set = RecoverySet {
        store: store.id(),
        index: index.token,
        log: log.token,
    };
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, _) = builder(config, true).recover(set).unwrap();
    for (logical, id, serial) in identities {
        let shared = store.clone();
        sessions.insert(
            logical,
            actor::Actor::new(move || {
                let resumed = shared.continue_session(id).unwrap();
                assert_eq!(resumed.progress.serial, serial);
                assert_eq!(resumed.session.last_accepted(), Some(serial));
                resumed.session
            }),
        );
    }
    println!(
        "Rust 轨迹边界恢复通过：模式 {}，{} 个会话续接",
        if pair { "Index+Log" } else { "Full" },
        sessions.len()
    );
    store
}
#[test]
fn 同一拥有型轨迹输出真实引擎结果供上游对照() {
    let trace = match std::env::var_os("RASTER_UPSTREAM_TRACE") {
        Some(path) => Trace::decode(&std::fs::read_to_string(path).unwrap()).unwrap(),
        None => Trace::decode(include_str!("fixtures/p0.trace")).unwrap(),
    };
    if std::env::var("RASTER_UPSTREAM_GENERATED").as_deref() == Ok("1") {
        assert_eq!(
            trace,
            Trace::generate(trace.seed, trace.steps.len()),
            "跨语言轨迹生成与固定算法不一致"
        );
    }
    let mut config = Config::default();
    let native = if let Some(root) = std::env::var_os("RASTER_UPSTREAM_ROOT") {
        config.storage.root = root.into();
        config.storage.segment_bytes = 1048576;
        config.log.page_bytes = 32768;
        config.log.memory_pages = 4;
        config.index.buckets = 2048;
        true
    } else {
        false
    };
    let split = std::env::var("RASTER_UPSTREAM_SPLIT")
        .ok()
        .map(|value| value.parse::<usize>().unwrap());
    if let Some(split) = split {
        assert!(native && split > 0 && split < trace.steps.len());
    }
    let pair = match std::env::var("RASTER_UPSTREAM_CHECKPOINT").as_deref() {
        Ok("pair") => true,
        Ok("full") | Err(_) => false,
        other => panic!("未知检查点模式 {other:?}"),
    };
    let mut store = builder(config.clone(), native).create().unwrap();
    let mut sessions = Sessions::new();
    let mut model = Model::default();
    let mut output = format!("raster-results 1 {}\n", trace.seed);
    let mut pending = [0usize; 4];
    for (index, step) in trace.steps.iter().enumerate() {
        let session = sessions.entry(step.session).or_insert_with(|| {
            let store = store.clone();
            actor::Actor::new(move || store.start_session(SessionOptions::default()).unwrap())
        });
        let expected = model.submit(step.clone());
        let input = step.clone();
        let (observed, counts, serial) = session.call(move |session| {
            let mut pending = [0; 4];
            let observed = replay::actual(session, &input, &mut pending);
            (observed, pending, session.last_accepted())
        });
        assert_eq!(observed, expected, "种子 {} 步骤 {index}", trace.seed);
        assert_eq!(serial.map(|s| s.0), model.last_accepted(step.session));
        for (sum, count) in pending.iter_mut().zip(counts) {
            *sum += count;
        }
        writeln!(
            output,
            "{} {} {}",
            step.session,
            step.serial,
            encode(&observed)
        )
        .unwrap();
        if split == Some(index + 1) {
            store = recover_boundary(store, &mut sessions, config.clone(), pair);
        }
    }
    for session in sessions.values() {
        session.call(|session| {
            session
                .close(Deadline(Instant::now() + Duration::from_secs(60)))
                .unwrap()
        });
    }
    drop(sessions);
    store
        .shutdown(Deadline(Instant::now() + Duration::from_secs(60)))
        .unwrap();
    if let Some(path) = std::env::var_os("RASTER_UPSTREAM_RESULT") {
        std::fs::write(path, output).unwrap();
    }
    println!(
        "Rust 轨迹逐操作通过：种子 {}，{} 次操作，Pending {:?}",
        trace.seed,
        trace.steps.len(),
        pending
    );
}
