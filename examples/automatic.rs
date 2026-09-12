//! Start automatic compression through the public interface, wait for one real
//! completion, stop it again, then exercise checkpoints and shutdown.
#[path = "disk_lifecycle/control.rs"]
mod control;
#[path = "disk_lifecycle/operation.rs"]
mod operation;
use control::{Result, deadline, report, shutdown, success, take};
use operation::{Request, Schema};
use raster::{
    RasterKV,
    api::{
        maintenance::{AutoCompactionPhase, CheckpointKind},
        operation::ReadOptions,
    },
    config::Config,
    device::thread_pool::ThreadPoolDeviceFactory,
    schema::builtin::{ByteKey, ByteValueCodec, SchemaPair, SerializedValue},
    types::*,
};
use std::{collections::BTreeMap, time::Duration};

fn scenario(store: &RasterKV<Schema>) -> Result<()> {
    let mut session = store.start_session(Default::default())?;
    let mut expected = BTreeMap::new();
    for serial in 0..800u64 {
        let key = vec![(serial % 32) as u8];
        let value = vec![(serial / 32) as u8; 768];
        let mut request = Request {
            key: key.clone(),
            value: value.clone(),
        };
        let end = deadline();
        let submitted = loop {
            match session.upsert(Serial(serial), request) {
                Ok(submitted) => break submitted,
                Err(rejected) => {
                    if !matches!(rejected.reason, Error::Busy) {
                        return Err(rejected.reason.into());
                    }
                    if end.expired() {
                        return Err(Error::DeadlineExceeded.into());
                    }
                    // Only requests that have not yet been accepted are retried here,Keep original input and serial number;Invoice delivery accepted take.
                    request = rejected.request;
                    session.poll(PollBudget::default())?;
                    std::thread::yield_now();
                }
            }
        };
        success(take(&mut session, submitted)?)?;
        expected.insert(key, value);
    }
    let end = deadline();
    loop {
        let status = store.maintenance().auto_compaction_status()?;
        if matches!(status.phase, AutoCompactionPhase::Failed) {
            return Err(format!("Automatic maintenance failed:{status:?}").into());
        }
        if status.completed_compactions > 0 {
            break;
        }
        if end.expired() {
            return Err(Error::DeadlineExceeded.into());
        }
        session.poll(PollBudget::default())?;
        std::thread::sleep(Duration::from_millis(1));
    }
    store.maintenance().stop_auto_compaction()?;
    let stopped = session.wait_auto_compaction(deadline())?;
    if stopped.phase != AutoCompactionPhase::Stopped
        || stopped.active.is_some()
        || stopped.failure.is_some()
    {
        return Err(format!("Automatic task did not stop normally:{stopped:?}").into());
    }
    if stopped
        .last_compaction
        .as_ref()
        .is_none_or(|result| result.is_err())
        || stopped
            .last_reclamation
            .as_ref()
            .is_some_and(|result| result.is_err())
    {
        return Err(format!("Automatic maintenance end report contains errors:{stopped:?}").into());
    }
    for (offset, (key, expected)) in expected.iter().enumerate() {
        let submitted = session
            .read(
                Serial(800 + offset as u64),
                Request::key(key.clone()),
                ReadOptions::default(),
            )
            .map_err(|rejected| rejected.reason)?;
        if success(take(&mut session, submitted)?)? != *expected {
            return Err("Latest value does not match after automatic compression".into());
        }
    }
    let checkpoint = report(
        &mut session,
        &store.maintenance().checkpoint(CheckpointKind::Full)?,
    )?;
    if checkpoint.sessions.is_empty() || checkpoint.begin == LogAddress(0) {
        return Err(
            "Automatic maintenance does not result in real truncation or checkpoint progress"
                .into(),
        );
    }
    println!(
        "Automated maintenance lifecycle passes:completed {} sub-compression,Verify after stopping 32 latest variable length value,Log [{}..{}),Explicit checkpoint committed.",
        stopped.completed_compactions, checkpoint.begin.0, checkpoint.end.0
    );
    session.close(deadline())?;
    Ok(())
}
fn main() -> Result<()> {
    let root =
        std::env::temp_dir().join(format!("raster-auto-example-{:x?}", StoreId::generate()?.0));
    std::fs::create_dir(&root)?;
    let mut config = Config::default();
    config.storage.root = root.clone();
    config.storage.segment_bytes = 16384;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    config.maintenance.auto_compaction = true;
    config.maintenance.workers = 2;
    config.maintenance.auto_compaction_policy.check_interval = Duration::from_millis(5);
    config.maintenance.auto_compaction_policy.log_size_budget = 65536;
    config
        .maintenance
        .auto_compaction_policy
        .max_compacted_bytes = 32768;
    config.statistics.enabled = true;
    let store = RasterKV::builder(SchemaPair::new(
        ByteKey,
        SerializedValue::new(ByteValueCodec),
    ))
    .config(config)
    .device(Box::new(ThreadPoolDeviceFactory {
        workers: 2,
        queue_capacity: 128,
    }))
    .create()?;
    let result = scenario(&store);
    let cleanup = shutdown(&store);
    match (result, cleanup) {
        (Ok(()), Ok(())) => {
            drop(store);
            std::fs::remove_dir_all(root)?;
            Ok(())
        }
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(format!(
            "Automatic maintenance process failed:{error};Closing failed:{cleanup}"
        )
        .into()),
    }
}
