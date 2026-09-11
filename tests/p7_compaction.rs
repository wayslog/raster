//! 仅使用公开接口验证空键、变长值、墓碑、重复压缩及释放旧检查点后恢复。
#![cfg(any(target_os = "linux", target_os = "macos"))]
use raster::{
    RasterKV, Session, Submission,
    api::{
        maintenance::*,
        operation::*,
        scan::{Buffering, ScanOptions},
    },
    config::Config,
    device::thread_pool::ThreadPoolDeviceFactory,
    schema::{
        ValueRead, ValueUpdate,
        builtin::{ByteKey, ByteValueCodec, SchemaPair, SerializedValue},
    },
    types::*,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{Duration, Instant},
};
type Schema = SchemaPair<ByteKey, SerializedValue<ByteValueCodec>>;
#[derive(Debug)]
struct Write(Vec<u8>, Vec<u8>);
impl Keyed<Schema> for Write {
    fn key(&self) -> &[u8] {
        &self.0
    }
}
impl UpsertOperation<Schema> for Write {
    type Output = ();
    fn replacement(&mut self) -> Result<(Vec<u8>, ()), Error> {
        Ok((self.1.clone(), ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[derive(Debug)]
struct Key(Vec<u8>);
impl Keyed<Schema> for Key {
    fn key(&self) -> &[u8] {
        &self.0
    }
}
impl DeleteOperation<Schema> for Key {
    type Output = ();
    fn complete(self, _: DeleteOutcome) {}
}
impl ReadOperation<Schema> for Key {
    type Output = Vec<u8>;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<Self::Output, Error> {
        Ok(value.view().clone())
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(20))
}
fn take<T: 'static>(
    session: &mut Session<Schema>,
    submission: Submission<T>,
) -> raster::api::completion::Outcome<T> {
    match submission {
        Submission::Ready(result) => result.unwrap(),
        Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline()).unwrap().unwrap(),
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
fn builder(config: Config) -> raster::api::Builder<Schema> {
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
#[test]
fn 两算法重复压缩变长数据再检查点恢复保持墓碑和会话切分() {
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-compaction-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.storage.segment_bytes = 8192;
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    let store = builder(config.clone()).create().unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    let mut expected = BTreeMap::new();
    let mut serial = 0;
    for generation in 0..2 {
        for number in 0u8..30 {
            let key = vec![number; usize::from(number)];
            let value = vec![255 - number; usize::from(number) * 31 + generation * 7];
            let submission = session
                .upsert(Serial(serial), Write(key.clone(), value.clone()))
                .unwrap();
            take(&mut session, submission);
            expected.insert(key, Some(value));
            serial += 1;
        }
    }
    for number in 0u8..5 {
        let key = vec![number; usize::from(number)];
        let submission = session
            .delete(Serial(serial), Key(key.clone()), Default::default())
            .unwrap();
        take(&mut session, submission);
        expected.insert(key, None);
        serial += 1;
    }
    let mut prefix = checkpoint(&store, &mut session);
    let mut retired_tokens = vec![prefix.token];
    let mut copied_begin = prefix.end;
    for algorithm in [CompactionAlgorithm::ScanDedup, CompactionAlgorithm::Lookup] {
        copied_begin = prefix.end;
        let ticket = store
            .maintenance()
            .compact(CompactionOptions {
                algorithm,
                until: prefix.end,
                workers: 1,
                shift_begin: false,
                checkpoint: false,
            })
            .unwrap();
        let report = session.wait_maintenance(&ticket, deadline()).unwrap();
        assert_eq!(report.as_ref().as_ref().unwrap().copied, 30);
        let current = checkpoint(&store, &mut session);
        retired_tokens.push(current.token);
        assert_eq!(current.begin, prefix.begin);
        let mut scan = store
            .scan(ScanOptions {
                begin: prefix.end,
                end: current.end,
                buffering: Buffering::DoublePage,
            })
            .unwrap();
        let mut observed = BTreeMap::new();
        while let Some(record) = scan.next_record().unwrap() {
            assert!(observed.insert(record.key, record.value).is_none());
        }
        assert_eq!(observed, expected);
        scan.close().unwrap();
        prefix = current;
    }
    let gc = store.maintenance().shift_begin(copied_begin).unwrap();
    let gc = session.wait_maintenance(&gc, deadline()).unwrap();
    let gc = gc.as_ref().as_ref().unwrap();
    assert_eq!(gc.begin, copied_begin);
    assert!(gc.index_cleaned);
    assert!(matches!(gc.physical, PhysicalReclamation::Completed));
    prefix = checkpoint(&store, &mut session);
    for token in retired_tokens {
        let release = store.maintenance().release_checkpoint(token).unwrap();
        let result = session.wait_maintenance(&release, deadline()).unwrap();
        let report = result.as_ref().as_ref().unwrap();
        assert_eq!(report.retirement, CheckpointRetirement::Retired);
        assert!(report.confirmed_absent_materials > 0);
        assert!(matches!(report.physical, PhysicalReclamation::Completed));
    }
    let session_id = session.id();
    assert_eq!(
        prefix
            .sessions
            .iter()
            .find(|cut| cut.session == session_id)
            .unwrap()
            .serial,
        Serial(serial - 1)
    );
    let set = RecoverySet {
        store: store.id(),
        index: prefix.token,
        log: prefix.token,
    };
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, _) = builder(config).recover(set).unwrap();
    let mut session = store.continue_session(session_id).unwrap().session;
    for (key, value) in expected {
        let submission = session
            .read(Serial(serial), Key(key), Default::default())
            .unwrap();
        let actual = match take(&mut session, submission) {
            raster::api::completion::Outcome::Success(value) => Some(value),
            raster::api::completion::Outcome::NotFound => None,
            _ => panic!("读取结果错误"),
        };
        assert_eq!(actual, value);
        serial += 1;
    }
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
