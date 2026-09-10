//! 公开检查点入口与原生材料的贯通验证；恢复入口另行验收。
use crate::{
    RasterKV, Submission,
    api::{
        maintenance::{CheckpointKind, CheckpointReport, MaintenanceTicket},
        operation::{Keyed, UpdateDecision, UpsertOperation},
        session::{Session, SessionOptions},
    },
    config::Config,
    device::{self, *},
    format::{Commit, IndexSnapshot, Kind, Manifest, PageFrame},
    schema::{
        ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[derive(Debug)]
struct Put(u64);
impl Keyed<Schema> for Put {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.0, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
fn setup(factory: Option<Box<dyn DeviceFactory>>) -> (Directory, RasterKV<Schema>) {
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-checkpoint-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    config.session.max_pending = 1;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(factory.unwrap_or_else(|| {
            Box::new(device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 16,
            })
        }))
        .create()
        .unwrap();
    (root, store)
}
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(10))
}
fn wait(
    session: &mut Session<Schema>,
    ticket: &MaintenanceTicket<CheckpointReport>,
) -> CheckpointReport {
    let report = session.wait_maintenance(ticket, deadline()).unwrap();
    report.as_ref().as_ref().unwrap().clone()
}
fn manifest(store: &RasterKV<Schema>, report: &CheckpointReport) -> Manifest {
    let read = |name: &str| {
        std::fs::read(
            store.inner.storage.root.join(
                store
                    .inner
                    .storage
                    .checkpoint_path(report.token, name)
                    .unwrap(),
            ),
        )
        .unwrap()
    };
    let manifest = Commit::decode(&read("commit"))
        .unwrap()
        .verify(&read("manifest"))
        .unwrap();
    assert_eq!(manifest.token, report.token);
    assert_eq!(manifest.version, report.version);
    for material in &manifest.materials {
        let bytes = read(&crate::storage::SegmentedStorage::checkpoint_material_name(
            material.id,
            material.generation,
        ));
        material.verify(&bytes).unwrap();
        match material.kind {
            Kind::Index => {
                IndexSnapshot::decode(&bytes).unwrap();
            }
            Kind::Log => {
                PageFrame::decode(&bytes, PageId(material.begin.0 / 4096), 4096)
                    .unwrap()
                    .records()
                    .unwrap();
            }
            Kind::Full => panic!("材料类型错误"),
        }
    }
    manifest
}
#[test]
fn 公开索引日志及完整检查点依次同步发布且会话切分真实() {
    let (_root, store) = setup(None);
    assert!(matches!(
        store.maintenance().checkpoint(CheckpointKind::Log),
        Err(Error::InvalidState(_))
    ));
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    assert!(matches!(
        session.upsert(Serial(7), Put(11)).unwrap(),
        Submission::Ready(Ok(_))
    ));
    let index = store
        .maintenance()
        .checkpoint(CheckpointKind::Index)
        .unwrap();
    assert!(matches!(
        store.maintenance().checkpoint(CheckpointKind::Full),
        Err(Error::Busy)
    ));
    let report = wait(&mut session, &index);
    assert!(report.sessions.is_empty());
    let initial = manifest(&store, &report);
    assert_eq!(initial.kind, Kind::Index);
    assert_eq!(initial.materials.len(), 1);
    assert_eq!(initial.version, CheckpointVersion(0));
    assert!(matches!(
        session.upsert(Serial(19), Put(22)).unwrap(),
        Submission::Ready(Ok(_))
    ));
    let log = store.maintenance().checkpoint(CheckpointKind::Log).unwrap();
    let report = wait(&mut session, &log);
    let log_manifest = manifest(&store, &report);
    assert_eq!(log_manifest.base_index, initial.token);
    assert_eq!(log_manifest.kind, Kind::Log);
    assert_eq!(report.sessions.len(), 1);
    assert_eq!(report.sessions[0].serial, Serial(19));
    assert_eq!(report.version, CheckpointVersion(0));
    assert!(log_manifest.materials.iter().all(|m| m.kind == Kind::Log));
    assert!(matches!(
        session.upsert(Serial(100), Put(33)).unwrap(),
        Submission::Ready(Ok(_))
    ));
    let full = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let report = wait(&mut session, &full);
    let full_manifest = manifest(&store, &report);
    assert_eq!(full_manifest.kind, Kind::Full);
    assert_eq!(report.version, CheckpointVersion(1));
    assert_eq!(report.sessions[0].serial, Serial(100));
    assert!(full_manifest.materials.len() >= 3);
    assert!(std::sync::Arc::ptr_eq(
        &full.try_report().unwrap().unwrap(),
        &full.try_report().unwrap().unwrap()
    ));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 空存储完整检查点和丢弃票据后推进均可结束() {
    let (_root, store) = setup(None);
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let end = deadline();
    while ticket.try_report().unwrap().is_none() {
        assert!(!end.expired());
        store.maintenance().poll(PollBudget::default()).unwrap();
    }
    let report = ticket.try_report().unwrap().unwrap();
    let report = report.as_ref().as_ref().unwrap();
    assert_eq!(report.end, LogAddress(0));
    assert!(report.sessions.is_empty());
    manifest(&store, report);
    drop(
        store
            .maintenance()
            .checkpoint(CheckpointKind::Index)
            .unwrap(),
    );
    while store.inner.coordinator.snapshot().unwrap().id.is_some() {
        assert!(!end.expired());
        store.maintenance().poll(PollBudget::default()).unwrap();
    }
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 检查点等待超时保留动作且会话放弃使已接受票据失败() {
    let (_root, store) = setup(None);
    let mut first = store.start_session(SessionOptions::default()).unwrap();
    let second = store.start_session(SessionOptions::default()).unwrap();
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    assert!(matches!(
        first.wait_maintenance(&ticket, Deadline(Instant::now())),
        Err(Error::DeadlineExceeded)
    ));
    assert!(ticket.try_report().unwrap().is_none());
    drop(second);
    assert!(store.maintenance().poll(PollBudget::default()).is_err());
    assert!(ticket.try_report().unwrap().unwrap().is_err());
    drop(first);
    store.shutdown(deadline()).unwrap();
}

#[derive(Debug)]
struct Read(u64);
impl Keyed<Schema> for Read {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl crate::api::operation::ReadOperation<Schema> for Read {
    type Output = u64;
    fn read(&mut self, value: crate::schema::ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*value.view())
    }
}
fn put(session: &mut Session<Schema>, serial: u64, key: u64) {
    match session.upsert(Serial(serial), Put(key)).unwrap() {
        Submission::Ready(result) => {
            result.unwrap();
        }
        Submission::Pending(mut ticket) => {
            session.wait(&mut ticket, deadline()).unwrap().unwrap();
        }
    }
}
#[test]
fn 完整检查点覆盖冷页和旧挂起请求但不承诺新版本序号() {
    let (_root, store) = setup(None);
    let mut closed = store.start_session(SessionOptions::default()).unwrap();
    put(&mut closed, 37, 999);
    closed.close(deadline()).unwrap();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    assert!(store.inner.log.frontiers().unwrap().head > LogAddress(0));
    let Submission::Pending(mut read) = session
        .read(Serial(400), Read(0), Default::default())
        .unwrap()
    else {
        panic!("旧键应从磁盘挂起读取")
    };
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let end = deadline();
    while store.inner.coordinator.snapshot().unwrap().phase
        != crate::coordination::Phase::InProgress
    {
        assert!(!end.expired());
        session.poll(PollBudget::default()).unwrap();
        store
            .maintenance()
            .poll(PollBudget(std::num::NonZeroUsize::new(1).unwrap()))
            .unwrap();
    }
    session.refresh().unwrap();
    session
        .complete_pending(WaitMode::Until(deadline()))
        .unwrap();
    put(&mut session, 401, 401);
    let report = wait(&mut session, &ticket);
    let manifest = manifest(&store, &report);
    let mut serials: Vec<_> = report.sessions.iter().map(|p| p.serial.0).collect();
    serials.sort();
    assert_eq!(serials, [37, 400]);
    assert_eq!(report.version, CheckpointVersion(0));
    assert!(matches!(
        session.wait(&mut read, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(0)
    ));
    let mut old_keys = std::collections::BTreeSet::new();
    let mut new_keys = std::collections::BTreeSet::new();
    for material in manifest.materials.iter().filter(|m| m.kind == Kind::Log) {
        let name = crate::storage::SegmentedStorage::checkpoint_material_name(
            material.id,
            material.generation,
        );
        let bytes = std::fs::read(
            store.inner.storage.root.join(
                store
                    .inner
                    .storage
                    .checkpoint_path(report.token, &name)
                    .unwrap(),
            ),
        )
        .unwrap();
        let frame = PageFrame::decode(&bytes, PageId(material.begin.0 / 4096), 4096).unwrap();
        for (_, record) in frame.records().unwrap() {
            let key = u64::from_le_bytes(record.key.try_into().unwrap());
            assert_eq!(u64::from_le_bytes(record.value.try_into().unwrap()), key);
            if record.header.version <= report.version {
                old_keys.insert(key);
            } else {
                new_keys.insert(key);
            }
        }
    }
    assert_eq!(old_keys, (0..400).chain([999]).collect());
    assert!(new_keys.contains(&401));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

struct FinalSyncFaultFactory;
struct FinalSyncFault {
    inner: Box<dyn Device>,
    renamed: std::sync::atomic::AtomicBool,
}
impl DeviceFactory for FinalSyncFaultFactory {
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(FinalSyncFault {
            inner: device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 16,
            }
            .open(options)?,
            renamed: false.into(),
        }))
    }
}
impl Device for FinalSyncFault {
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        use std::sync::atomic::Ordering::SeqCst;
        if matches!(request.operation, IoOperation::SyncDirectory(_)) && self.renamed.load(SeqCst) {
            return Err(RejectedIo {
                request,
                reason: std::io::Error::other("注入提交后目录同步失败").into(),
            });
        }
        let rename = matches!(request.operation, IoOperation::Rename { .. });
        let id = self.inner.submit(request)?;
        if rename {
            self.renamed.store(true, SeqCst);
        }
        Ok(id)
    }
    fn poll(&self, budget: PollBudget, out: &mut Vec<IoCompletion>) -> Result<(), Error> {
        self.inner.poll(budget, out)
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.inner.shutdown(deadline)
    }
}
#[test]
fn 提交可见但最终目录同步失败时公开票据保持失败() {
    let (root, store) = setup(Some(Box::new(FinalSyncFaultFactory)));
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let end = deadline();
    while ticket.try_report().unwrap().is_none() {
        assert!(!end.expired());
        let _ = store.maintenance().poll(PollBudget::default());
    }
    let first = ticket.try_report().unwrap().unwrap();
    assert!(matches!(first.as_ref(), Err(Error::Io(_))));
    assert!(
        store
            .maintenance()
            .checkpoint(CheckpointKind::Index)
            .is_err()
    );
    store.shutdown(deadline()).unwrap();
    assert!(std::sync::Arc::ptr_eq(
        &first,
        &ticket.try_report().unwrap().unwrap()
    ));
    let directory = std::fs::read_dir(root.0.join("checkpoints"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let commit = std::fs::read(directory.join("commit")).unwrap();
    Commit::decode(&commit)
        .unwrap()
        .verify(&std::fs::read(directory.join("manifest")).unwrap())
        .unwrap();
}

#[test]
fn 不支持持久化的设备在接受检查点前拒绝且不占用动作() {
    let (_root, store) = setup(Some(Box::new(device::null::NullDeviceFactory)));
    for kind in [
        CheckpointKind::Full,
        CheckpointKind::Index,
        CheckpointKind::Log,
    ] {
        assert!(matches!(
            store.maintenance().checkpoint(kind),
            Err(Error::UnsupportedDurability)
        ));
        assert!(store.inner.coordinator.snapshot().unwrap().id.is_none());
    }
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    put(&mut session, 1, 7);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn 多线程推进检查点只报告一次完成且动作期间拒绝关闭() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    };
    let (_root, store) = setup(None);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    put(&mut session, 17, 91);
    let ticket = Arc::new(
        store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    session.close(deadline()).unwrap();
    assert!(matches!(store.shutdown(deadline()), Err(Error::Busy)));
    let completed = Arc::new(AtomicUsize::new(0));
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let store = store.clone();
            let ticket = ticket.clone();
            let completed = completed.clone();
            std::thread::spawn(move || {
                let end = deadline();
                while ticket.try_report().unwrap().is_none() {
                    assert!(!end.expired());
                    completed.fetch_add(
                        store
                            .maintenance()
                            .poll(PollBudget::default())
                            .unwrap()
                            .completed,
                        SeqCst,
                    );
                    std::thread::yield_now();
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(completed.load(SeqCst), 1);
    let report = ticket.try_report().unwrap().unwrap();
    let report = report.as_ref().as_ref().unwrap();
    assert_eq!(report.sessions[0].serial, Serial(17));
    manifest(&store, report);
    assert_eq!(
        store
            .maintenance()
            .poll(PollBudget::default())
            .unwrap()
            .completed,
        0
    );
    store.shutdown(deadline()).unwrap();
}
