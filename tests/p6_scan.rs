//! 公开扫描以检查点报告指定范围，验证变长记录、旧版本、墓碑和恢复后的相同物理历史。
#![cfg(any(target_os = "linux", target_os = "macos"))]
use raster::{
    RasterKV, Submission,
    api::{
        maintenance::{CheckpointKind, CheckpointReport, RecoverySet},
        operation::*,
        scan::{Buffering, ScanOptions},
        session::{Session, SessionOptions},
    },
    config::Config,
    device::thread_pool::ThreadPoolDeviceFactory,
    schema::{
        ValueUpdate,
        builtin::{ByteKey, ByteValueCodec, SchemaPair, SerializedValue},
    },
    types::*,
};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
type Schema = SchemaPair<ByteKey, SerializedValue<ByteValueCodec>>;
#[derive(Debug)]
struct Put(Vec<u8>, Vec<u8>);
impl Keyed<Schema> for Put {
    fn key(&self) -> &[u8] {
        &self.0
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = ();
    fn replacement(&mut self) -> Result<(Vec<u8>, ()), Error> {
        Ok((self.1.clone(), ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[derive(Debug)]
struct Delete(Vec<u8>);
impl Keyed<Schema> for Delete {
    fn key(&self) -> &[u8] {
        &self.0
    }
}
impl DeleteOperation<Schema> for Delete {
    type Output = ();
    fn complete(self, _: DeleteOutcome) {}
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(15))
}
fn finish(session: &mut Session<Schema>, submission: Submission<()>) {
    match submission {
        Submission::Ready(result) => {
            result.unwrap();
        }
        Submission::Pending(mut ticket) => {
            session.wait(&mut ticket, deadline()).unwrap().unwrap();
        }
    }
}
fn checkpoint(store: &RasterKV<Schema>, session: &mut Session<Schema>) -> CheckpointReport {
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    session
        .wait_maintenance(&ticket, deadline())
        .unwrap()
        .as_ref()
        .as_ref()
        .unwrap()
        .clone()
}
fn builder(config: Config) -> raster::Builder<Schema> {
    RasterKV::builder(SchemaPair::new(
        ByteKey,
        SerializedValue::new(ByteValueCodec),
    ))
    .config(config)
    .device(Box::new(ThreadPoolDeviceFactory {
        workers: 2,
        queue_capacity: 16,
    }))
}
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[derive(Debug, PartialEq, Eq)]
struct Expected {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    tombstone: bool,
    version: CheckpointVersion,
}
fn scan(store: &RasterKV<Schema>, report: &CheckpointReport, mode: Buffering) -> Vec<Expected> {
    let mut scanner = store
        .scan(ScanOptions {
            begin: report.begin,
            end: report.end,
            buffering: mode,
        })
        .unwrap();
    let mut out = Vec::new();
    let mut previous = None;
    while let Some(record) = scanner.next_record().unwrap() {
        assert!(!record.invalid);
        if let Some(previous) = previous {
            assert!(record.address > previous);
        }
        previous = Some(record.address);
        out.push(Expected {
            key: record.key,
            value: record.value,
            tombstone: record.tombstone,
            version: record.version,
        });
    }
    scanner.close().unwrap();
    out
}
#[test]
fn 三模式返回变长物理历史且重启恢复后保持旧版本和墓碑() {
    let root = Directory(
        std::env::temp_dir().join(format!("raster-scan-{:x?}", StoreId::generate().unwrap().0)),
    );
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.storage.segment_bytes = 8192;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = builder(config.clone()).create().unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let key = |number: u8| {
        if number == 0 {
            vec![]
        } else {
            vec![0, 255, number]
        }
    };
    let mut expected = Vec::new();
    let mut serial = 0;
    for generation in 0..2 {
        for number in 0..20 {
            let value = if number == 0 {
                vec![]
            } else {
                vec![number + generation; 500 + usize::from(number) * 30]
            };
            let submission = session
                .upsert(Serial(serial), Put(key(number), value.clone()))
                .unwrap();
            finish(&mut session, submission);
            expected.push(Expected {
                key: key(number),
                value: Some(value),
                tombstone: false,
                version: CheckpointVersion(generation as u64),
            });
            serial += 1;
        }
        if generation == 0 {
            checkpoint(&store, &mut session);
        }
    }
    for number in 0..5 {
        let submission = session
            .delete(
                Serial(serial),
                Delete(key(number)),
                DeleteOptions {
                    force_tombstone: true,
                },
            )
            .unwrap();
        finish(&mut session, submission);
        expected.push(Expected {
            key: key(number),
            value: None,
            tombstone: true,
            version: CheckpointVersion(1),
        });
        serial += 1;
    }
    let report = checkpoint(&store, &mut session);
    session.close(deadline()).unwrap();
    let set = RecoverySet {
        store: store.id(),
        index: report.token,
        log: report.token,
    };
    let mut outputs = Vec::new();
    for mode in [
        Buffering::Unbuffered,
        Buffering::SinglePage,
        Buffering::DoublePage,
    ] {
        outputs.push(scan(&store, &report, mode));
    }
    store.shutdown(deadline()).unwrap();
    drop(store);
    for output in &outputs {
        assert_eq!(output, &expected);
    }
    let (restored, _) = builder(config).recover(set).unwrap();
    for mode in [
        Buffering::Unbuffered,
        Buffering::SinglePage,
        Buffering::DoublePage,
    ] {
        assert_eq!(scan(&restored, &report, mode), expected);
    }
    restored.shutdown(deadline()).unwrap();
}
