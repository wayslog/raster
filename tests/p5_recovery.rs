//! 使用公开入口验证变长键值恢复、继续读改写及再次检查点。
#![cfg(any(target_os = "linux", target_os = "macos"))]
use raster::{
    RasterKV, Submission,
    api::{
        completion::Outcome,
        maintenance::{CheckpointKind, RecoverySet},
        operation::*,
        session::{Session, SessionOptions},
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
    path::PathBuf,
    time::{Duration, Instant},
};
type Schema = SchemaPair<ByteKey, SerializedValue<ByteValueCodec>>;
#[derive(Debug)]
struct Context {
    key: Vec<u8>,
    value: Vec<u8>,
}
impl Keyed<Schema> for Context {
    fn key(&self) -> &[u8] {
        &self.key
    }
}
impl UpsertOperation<Schema> for Context {
    type Output = Vec<u8>;
    fn replacement(&mut self) -> Result<(Vec<u8>, Vec<u8>), Error> {
        Ok((self.value.clone(), self.value.clone()))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<Vec<u8>>, Error> {
        Ok(UpdateDecision::Append)
    }
}
impl ReadOperation<Schema> for Context {
    type Output = Vec<u8>;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<Vec<u8>, Error> {
        Ok(value.view().clone())
    }
}
impl RmwOperation<Schema> for Context {
    type Output = Vec<u8>;
    fn initial(&mut self) -> Result<(Vec<u8>, Vec<u8>), Error> {
        self.replacement()
    }
    fn copy_update(&mut self, value: ValueRead<'_, Schema>) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let mut result = value.view().clone();
        result.extend_from_slice(&self.value);
        Ok((result.clone(), result))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<Vec<u8>>, Error> {
        Ok(UpdateDecision::Append)
    }
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(10))
}
fn outcome(session: &mut Session<Schema>, submission: Submission<Vec<u8>>) -> Vec<u8> {
    let result = match submission {
        Submission::Ready(result) => result,
        Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline()).unwrap(),
    };
    match result.unwrap() {
        Outcome::Success(value) => value,
        _ => panic!("预期成功结果"),
    }
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
#[test]
fn 变长字节键值恢复后读改写扩展且重新恢复保持结果() {
    scenario(false);
}
#[test]
fn 启用读缓存的变长字节值命中后可读改写并再次恢复() {
    scenario(true);
}
fn scenario(cache: bool) {
    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-byte-recovery-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 2;
    config.cache.enabled = cache;
    config.cache.capacity_bytes = 128 * 1024;
    let store = builder(config.clone()).create().unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let id = session.id();
    for key in 0u8..30 {
        let value = vec![key; 1000 + (key as usize % 3) * 300];
        let submission = session
            .upsert(
                Serial(key as u64),
                Context {
                    key: vec![key, 255],
                    value: value.clone(),
                },
            )
            .unwrap();
        assert_eq!(outcome(&mut session, submission), value);
    }
    let submission = session
        .upsert(
            Serial(30),
            Context {
                key: vec![],
                value: vec![],
            },
        )
        .unwrap();
    outcome(&mut session, submission);
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    let report = report.as_ref().as_ref().unwrap();
    let set = RecoverySet {
        store: store.id(),
        index: report.token,
        log: report.token,
    };
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, recovered) = builder(config.clone()).recover(set).unwrap();
    assert_eq!(recovered.sessions[0].serial, Serial(30));
    let mut session = store.continue_session(id).unwrap().session;
    let submission = session
        .read(
            Serial(31),
            Context {
                key: vec![],
                value: vec![],
            },
            Default::default(),
        )
        .unwrap();
    assert!(outcome(&mut session, submission).is_empty());
    let submission = session
        .read(
            Serial(32),
            Context {
                key: vec![7, 255],
                value: vec![],
            },
            Default::default(),
        )
        .unwrap();
    assert_eq!(outcome(&mut session, submission), vec![7; 1300]);
    let submission = session
        .read(
            Serial(33),
            Context {
                key: vec![7, 255],
                value: vec![],
            },
            Default::default(),
        )
        .unwrap();
    if cache {
        assert!(
            matches!(&submission, Submission::Ready(_)),
            "第二次读取应命中缓存"
        );
    }
    assert_eq!(outcome(&mut session, submission), vec![7; 1300]);
    let submission = session
        .rmw(
            Serial(34),
            Context {
                key: vec![7, 255],
                value: vec![255; 17],
            },
            Default::default(),
        )
        .unwrap();
    let expected = [vec![7; 1300], vec![255; 17]].concat();
    assert_eq!(outcome(&mut session, submission), expected);
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let report = session.wait_maintenance(&ticket, deadline()).unwrap();
    let report = report.as_ref().as_ref().unwrap();
    let set = RecoverySet {
        store: recovered.set.store,
        index: report.token,
        log: report.token,
    };
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, _) = builder(config).recover(set).unwrap();
    let mut session = store.continue_session(id).unwrap().session;
    let submission = session
        .read(
            Serial(35),
            Context {
                key: vec![7, 255],
                value: vec![],
            },
            Default::default(),
        )
        .unwrap();
    assert_eq!(outcome(&mut session, submission), expected);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
