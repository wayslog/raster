//! 公开检查点、恢复和原生材料的贯通验证。
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
    let mut replay =
        crate::checkpoint::replay::Replay::new(crate::checkpoint::replay::ReplayOptions {
            begin: report.begin,
            end: report.end,
            page_bytes: 4096,
            version: report.version,
            buckets: store.inner.config.index.buckets,
            generation: Generation(0),
            max_records: 1000,
        })
        .unwrap();
    let mut replayed = std::collections::BTreeMap::new();
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
        let rewritten = replay.page(&bytes, &U64Key).unwrap();
        let rewritten_frame =
            PageFrame::decode(&rewritten, PageId(material.begin.0 / 4096), 4096).unwrap();
        for (address, record) in rewritten_frame.records().unwrap() {
            assert!(record.header.version <= report.version);
            replayed.insert(
                address,
                (
                    u64::from_le_bytes(record.key.try_into().unwrap()),
                    u64::from_le_bytes(record.value.try_into().unwrap()),
                    record.header.previous,
                ),
            );
        }
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
    assert_eq!(replayed.len(), old_keys.len());
    let mut index = crate::index::MemIndex::new(store.inner.config.index.clone()).unwrap();
    index.restore(replay.index().unwrap()).unwrap();
    for key in old_keys.iter().copied().chain([401]) {
        let hash = crate::schema::KeyCodec::hash(&U64Key, &key);
        let mut address = index
            .locate(hash)
            .unwrap()
            .and_then(|entry| match entry.head {
                crate::index::IndexHead::Log(address) => Some(address),
                _ => None,
            });
        let mut found = None;
        while let Some(at) = address {
            let &(stored, value, previous) = replayed.get(&at).unwrap();
            if stored == key {
                found = Some(value);
                break;
            }
            address = previous;
        }
        assert_eq!(found, if key == 401 { None } else { Some(key) });
    }
    let config = store.inner.config.clone();
    let id = session.id();
    let set = crate::api::maintenance::RecoverySet {
        store: store.id(),
        index: report.token,
        log: report.token,
    };
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, recovered) = recover_store(config, set).unwrap();
    let mut cuts: Vec<_> = recovered.sessions.iter().map(|p| p.serial.0).collect();
    cuts.sort();
    assert_eq!(cuts, [37, 400]);
    let mut session = store.continue_session(id).unwrap().session;
    for (offset, key) in old_keys.into_iter().chain([401]).enumerate() {
        assert_eq!(
            read_value(&mut session, 402 + offset as u64, key),
            if key == 401 { None } else { Some(key) },
        );
    }
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

#[test]
fn 恢复计划逐材料验证且同一完整检查点不重复计数() {
    use crate::checkpoint::recovery::{RecoveryPlan, ValidatedMaterial};
    let (_root, store) = setup(None);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    put(&mut session, 7, 19);
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let report = wait(&mut session, &ticket);
    let manifest = manifest(&store, &report);
    let set = crate::api::maintenance::RecoverySet {
        store: store.inner.id,
        index: report.token,
        log: report.token,
    };
    let mut plan = RecoveryPlan::new(
        &set,
        manifest.clone(),
        manifest.clone(),
        &*store.inner.schema,
        &store.inner.config,
    )
    .unwrap();
    assert_eq!(plan.remaining(), manifest.materials.len());
    let requests: Vec<_> = plan
        .requests()
        .map(|(token, material)| (token, material.clone()))
        .collect();
    for (token, material) in requests {
        let name = crate::storage::SegmentedStorage::checkpoint_material_name(
            material.id,
            material.generation,
        );
        let bytes = std::fs::read(
            store
                .inner
                .storage
                .root
                .join(store.inner.storage.checkpoint_path(token, &name).unwrap()),
        )
        .unwrap();
        let before = plan.remaining();
        let mut bad = bytes.clone();
        bad[0] ^= 1;
        assert!(
            plan.verify_material(token, material.id, &bad, &*store.inner.schema)
                .is_err()
        );
        assert!(
            plan.verify_material(
                CheckpointToken([99; 16]),
                material.id,
                &bytes,
                &*store.inner.schema
            )
            .is_err()
        );
        assert_eq!(plan.remaining(), before);
        match plan
            .verify_material(token, material.id, &bytes, &*store.inner.schema)
            .unwrap()
        {
            ValidatedMaterial::Index(index) => assert_eq!(index.entries.len(), 1),
            ValidatedMaterial::Log(frame) => {
                assert_eq!(frame.records().unwrap()[0].1.value, 19u64.to_le_bytes())
            }
        }
        assert_eq!(plan.remaining(), before - 1);
        assert!(
            plan.verify_material(token, material.id, &bytes, &*store.inner.schema)
                .is_err()
        );
    }
    assert_eq!(plan.remaining(), 0);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 恢复计划拒绝错配语义页布局和越界索引链头() {
    use crate::checkpoint::recovery::RecoveryPlan;
    let (_root, store) = setup(None);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    put(&mut session, 7, 19);
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Index)
        .unwrap();
    let index_report = wait(&mut session, &ticket);
    let index = manifest(&store, &index_report);
    let ticket = store.maintenance().checkpoint(CheckpointKind::Log).unwrap();
    let report = wait(&mut session, &ticket);
    let log = manifest(&store, &report);
    let set = crate::api::maintenance::RecoverySet {
        store: store.inner.id,
        index: index.token,
        log: log.token,
    };
    assert!(
        RecoveryPlan::new(
            &set,
            index.clone(),
            log.clone(),
            &*store.inner.schema,
            &store.inner.config
        )
        .is_ok()
    );
    let mut bad = log.clone();
    bad.base_index = CheckpointToken([99; 16]);
    assert!(
        RecoveryPlan::new(
            &set,
            index.clone(),
            bad,
            &*store.inner.schema,
            &store.inner.config
        )
        .is_err()
    );
    for which in 0..3 {
        let mut a = index.clone();
        let mut b = log.clone();
        match which {
            0 => {
                a.key_format = FormatId([99; 16]);
                b.key_format = a.key_format;
            }
            1 => {
                a.hash.seed[0] ^= 1;
                b.hash = a.hash.clone();
            }
            _ => {
                a.value_format = FormatId([99; 16]);
                b.value_format = a.value_format;
            }
        }
        assert!(RecoveryPlan::new(&set, a, b, &*store.inner.schema, &store.inner.config).is_err());
    }
    let mut config = store.inner.config.clone();
    config.log.page_bytes *= 2;
    assert!(
        RecoveryPlan::new(
            &set,
            index.clone(),
            log.clone(),
            &*store.inner.schema,
            &config
        )
        .is_err()
    );
    let mut image = IndexSnapshot {
        buckets: store.inner.config.index.buckets as u64,
        generation: Generation(0),
        entries: vec![crate::format::IndexEntry {
            bucket: 0,
            tag: 0,
            address: index.end,
        }],
    };
    let bytes = image.encode().unwrap();
    let mut bad_index = index.clone();
    bad_index.materials[0].bytes = bytes.len() as u64;
    bad_index.materials[0].checksum = crate::format::checksum(&bytes);
    let mut plan = RecoveryPlan::new(
        &set,
        bad_index,
        log.clone(),
        &*store.inner.schema,
        &store.inner.config,
    )
    .unwrap();
    assert!(
        plan.verify_material(
            index.token,
            index.materials[0].id,
            &bytes,
            &*store.inner.schema
        )
        .is_err()
    );
    image.entries[0].address = LogAddress(0);
    image.buckets *= 2;
    let bytes = image.encode().unwrap();
    let mut bad_index = index;
    bad_index.materials[0].bytes = bytes.len() as u64;
    bad_index.materials[0].checksum = crate::format::checksum(&bytes);
    let mut plan = RecoveryPlan::new(
        &set,
        bad_index.clone(),
        log,
        &*store.inner.schema,
        &store.inner.config,
    )
    .unwrap();
    assert!(
        plan.verify_material(
            bad_index.token,
            bad_index.materials[0].id,
            &bytes,
            &*store.inner.schema
        )
        .is_err()
    );
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn 日志材料校验通过但键编码不规范时恢复计划不接受材料() {
    let (_root, store) = setup(None);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    put(&mut session, 1, 19);
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let report = wait(&mut session, &ticket);
    let mut manifest = manifest(&store, &report);
    let material = manifest
        .materials
        .iter_mut()
        .find(|m| m.kind == Kind::Log)
        .unwrap();
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
    let frame = PageFrame::decode(&bytes, PageId(0), 4096).unwrap();
    let records = frame.records().unwrap();
    let original = &records[0].1;
    let mut header = original.header.clone();
    header.key_bytes = 7;
    let mut payload = vec![0; 4096];
    let length = header.encoded_len().unwrap();
    crate::format::Record {
        header,
        key: &original.key[..7],
        value: original.value,
    }
    .encode(&mut payload[..length])
    .unwrap();
    let bytes = PageFrame {
        page: PageId(0),
        version: frame.version,
        payload: &payload,
    }
    .encode()
    .unwrap();
    material.checksum = crate::format::checksum(&bytes);
    material.verify(&bytes).unwrap();
    assert_eq!(
        PageFrame::decode(&bytes, PageId(0), 4096)
            .unwrap()
            .records()
            .unwrap()
            .len(),
        1
    );
    let id = material.id;
    let set = crate::api::maintenance::RecoverySet {
        store: store.inner.id,
        index: report.token,
        log: report.token,
    };
    let mut plan = crate::checkpoint::recovery::RecoveryPlan::new(
        &set,
        manifest.clone(),
        manifest,
        &*store.inner.schema,
        &store.inner.config,
    )
    .unwrap();
    let remaining = plan.remaining();
    assert!(matches!(
        plan.verify_material(report.token, id, &bytes, &*store.inner.schema),
        Err(Error::Codec(_))
    ));
    assert_eq!(plan.remaining(), remaining);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[derive(Debug)]
struct Delete(u64);
impl Keyed<Schema> for Delete {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl crate::api::operation::DeleteOperation<Schema> for Delete {
    type Output = ();
    fn complete(self, _: crate::api::operation::DeleteOutcome) {}
}
fn read_value(session: &mut Session<Schema>, serial: u64, key: u64) -> Option<u64> {
    let outcome = match session
        .read(Serial(serial), Read(key), Default::default())
        .unwrap()
    {
        Submission::Ready(result) => result.unwrap(),
        Submission::Pending(mut ticket) => session.wait(&mut ticket, deadline()).unwrap().unwrap(),
    };
    match outcome {
        crate::api::completion::Outcome::Success(value) => Some(value),
        crate::api::completion::Outcome::NotFound => None,
        _ => panic!("意外读取结果"),
    }
}
fn recover_store(
    config: Config,
    set: crate::api::maintenance::RecoverySet,
) -> Result<(RasterKV<Schema>, crate::api::maintenance::RecoveryReport), Error> {
    RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }))
        .recover(set)
}
#[test]
fn 完整恢复保留墓碑与进度并可继续写入再次检查点和恢复() {
    let (_root, store) = setup(None);
    let config = store.inner.config.clone();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let id = session.id();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    match session
        .delete(Serial(400), Delete(1), Default::default())
        .unwrap()
    {
        Submission::Ready(result) => {
            result.unwrap();
        }
        Submission::Pending(mut ticket) => {
            session.wait(&mut ticket, deadline()).unwrap().unwrap();
        }
    }
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let report = wait(&mut session, &ticket);
    let source_manifest = manifest(&store, &report);
    let source_path = store.inner.storage.root.join(
        store
            .inner
            .storage
            .checkpoint_path(report.token, "manifest")
            .unwrap(),
    );
    let source_bytes = std::fs::read(&source_path).unwrap();
    put(&mut session, 401, 999);
    session.close(deadline()).unwrap();
    drop(session);
    let set = crate::api::maintenance::RecoverySet {
        store: store.inner.id,
        index: report.token,
        log: report.token,
    };
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, recovered) = recover_store(config.clone(), set).unwrap();
    assert_eq!(recovered.version, report.version);
    assert_eq!(recovered.sessions[0].serial, Serial(400));
    assert!(
        store
            .inner
            .storage
            .segment_directory()
            .to_string_lossy()
            .starts_with("restore-")
    );
    assert!(
        store
            .start_session(SessionOptions { id: Some(id) })
            .is_err()
    );
    assert!(store.continue_session(SessionId([99; 16])).is_err());
    let resumed = store.continue_session(id).unwrap();
    assert_eq!(resumed.progress.serial, Serial(400));
    assert!(matches!(store.continue_session(id), Err(Error::Busy)));
    let mut session = resumed.session;
    assert!(session.upsert(Serial(400), Put(888)).is_err());
    assert_eq!(read_value(&mut session, 401, 0), Some(0));
    assert_eq!(read_value(&mut session, 402, 1), None);
    assert_eq!(read_value(&mut session, 403, 999), None);
    put(&mut session, 404, 777);
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let next = wait(&mut session, &ticket);
    assert_eq!(next.sessions[0].serial, Serial(404));
    assert_eq!(next.version, CheckpointVersion(report.version.0 + 1));
    assert_eq!(std::fs::read(&source_path).unwrap(), source_bytes);
    assert_eq!(source_manifest.token, report.token);
    session.close(deadline()).unwrap();
    drop(session);
    let set = crate::api::maintenance::RecoverySet {
        store: store.inner.id,
        index: next.token,
        log: next.token,
    };
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, _) = recover_store(config, set).unwrap();
    let mut session = store.continue_session(id).unwrap().session;
    assert_eq!(read_value(&mut session, 405, 777), Some(777));
    assert_eq!(read_value(&mut session, 406, 1), None);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[test]
fn 索引日志分离恢复后可以再次提交仅日志检查点() {
    let (_root, store) = setup(None);
    let config = store.inner.config.clone();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let id = session.id();
    put(&mut session, 10, 42);
    let index = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Index)
            .unwrap(),
    );
    put(&mut session, 20, 84);
    let log = wait(
        &mut session,
        &store.maintenance().checkpoint(CheckpointKind::Log).unwrap(),
    );
    let set = crate::api::maintenance::RecoverySet {
        store: store.inner.id,
        index: index.token,
        log: log.token,
    };
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let mut invalid = set.clone();
    invalid.log = invalid.index;
    assert!(recover_store(config.clone(), invalid).is_err());
    let (store, report) = recover_store(config.clone(), set.clone()).unwrap();
    assert_eq!(report.sessions[0].serial, Serial(20));
    let mut session = store.continue_session(id).unwrap().session;
    assert_eq!(read_value(&mut session, 21, 42), Some(42));
    assert_eq!(read_value(&mut session, 22, 84), Some(84));
    let next = wait(
        &mut session,
        &store.maintenance().checkpoint(CheckpointKind::Log).unwrap(),
    );
    assert_eq!(manifest(&store, &next).base_index, set.index);
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, report) = recover_store(
        config,
        crate::api::maintenance::RecoverySet {
            log: next.token,
            ..set
        },
    )
    .unwrap();
    assert_eq!(report.sessions[0].serial, Serial(22));
    let mut session = store.continue_session(id).unwrap().session;
    assert_eq!(read_value(&mut session, 23, 42), Some(42));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 恢复预算不足或材料缺失不发布实例且原提交保持不变() {
    let (root, store) = setup(None);
    let config = store.inner.config.clone();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..120 {
        put(&mut session, key, key);
    }
    let report = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    let manifest = manifest(&store, &report);
    let set = crate::api::maintenance::RecoverySet {
        store: store.inner.id,
        index: report.token,
        log: report.token,
    };
    let paths: Vec<_> = manifest
        .materials
        .iter()
        .map(|m| {
            root.0.join(
                store
                    .inner
                    .storage
                    .checkpoint_path(
                        report.token,
                        &crate::storage::SegmentedStorage::checkpoint_material_name(
                            m.id,
                            m.generation,
                        ),
                    )
                    .unwrap(),
            )
        })
        .collect();
    let original: Vec<_> = paths.iter().map(|p| std::fs::read(p).unwrap()).collect();
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let mut limited = config.clone();
    limited.recovery.max_records = 64;
    assert!(matches!(
        recover_store(limited, set.clone()),
        Err(Error::CapacityExceeded)
    ));
    for (path, bytes) in paths.iter().zip(&original) {
        assert_eq!(&std::fs::read(path).unwrap(), bytes);
    }
    let mut limited = config.clone();
    limited.recovery.max_index_bytes = 36;
    assert!(matches!(
        recover_store(limited, set.clone()),
        Err(Error::CapacityExceeded)
    ));
    let (store, _) = recover_store(config.clone(), set.clone()).unwrap();
    store.shutdown(deadline()).unwrap();
    drop(store);
    let wrong_schema = RasterKV::builder(crate::schema::builtin::SchemaPair::new(
        crate::schema::builtin::ByteKey,
        AtomicU64Value,
    ))
    .config(config.clone())
    .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
        workers: 2,
        queue_capacity: 16,
    }))
    .recover(set.clone());
    assert!(matches!(wrong_schema, Err(Error::InvalidFormat(_))));
    let mut damaged = original[0].clone();
    damaged[0] ^= 1;
    std::fs::write(&paths[0], &damaged).unwrap();
    assert!(matches!(
        recover_store(config.clone(), set.clone()),
        Err(Error::InvalidFormat(_))
    ));
    std::fs::write(&paths[0], &original[0]).unwrap();
    std::fs::remove_file(paths.last().unwrap()).unwrap();
    assert!(matches!(recover_store(config, set), Err(Error::Io(_))));
}
#[test]
fn 恢复子进程入口() {
    let Some(root) = std::env::var_os("RASTER_RECOVERY_CHILD_ROOT") else {
        return;
    };
    let decode = |name| {
        let text = std::env::var(name).unwrap();
        let mut bytes = [0; 16];
        assert_eq!(text.len(), 32);
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).unwrap();
        }
        bytes
    };
    let mut config = Config::default();
    config.storage.root = root.into();
    config.log.page_bytes = 4096;
    let token = CheckpointToken(decode("RASTER_RECOVERY_CHILD_TOKEN"));
    let (store, _) = recover_store(
        config,
        crate::api::maintenance::RecoverySet {
            store: StoreId(decode("RASTER_RECOVERY_CHILD_STORE")),
            index: token,
            log: token,
        },
    )
    .unwrap();
    let mut session = store
        .continue_session(SessionId(decode("RASTER_RECOVERY_CHILD_SESSION")))
        .unwrap()
        .session;
    assert_eq!(read_value(&mut session, 8, 19), Some(19));
    put(&mut session, 9, 23);
    let report = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    assert_eq!(report.sessions[0].serial, Serial(9));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    println!("恢复子进程完成");
}
#[test]
fn 独立进程恢复后读取写入及检查点通过() {
    let (root, store) = setup(None);
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let id = session.id();
    put(&mut session, 7, 19);
    let report = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    let store_id = store.inner.id;
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let hex = |bytes: [u8; 16]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "engine::checkpoint_tests::恢复子进程入口",
            "--nocapture",
        ])
        .env("RASTER_RECOVERY_CHILD_ROOT", &root.0)
        .env("RASTER_RECOVERY_CHILD_TOKEN", hex(report.token.0))
        .env("RASTER_RECOVERY_CHILD_STORE", hex(store_id.0))
        .env("RASTER_RECOVERY_CHILD_SESSION", hex(id.0))
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "子进程失败：{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        String::from_utf8(result.stdout)
            .unwrap()
            .contains("恢复子进程完成")
    );
}

struct RecoverySyncFaultFactory(std::sync::Arc<std::sync::atomic::AtomicBool>);
struct RecoverySyncFault {
    inner: Box<dyn Device>,
    stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl DeviceFactory for RecoverySyncFaultFactory {
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(RecoverySyncFault {
            inner: device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 16,
            }
            .open(options)?,
            stopped: self.0.clone(),
        }))
    }
}
impl Device for RecoverySyncFault {
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        if matches!(&request.operation, IoOperation::SyncDirectory(path) if path.to_string_lossy().starts_with("restore-"))
        {
            return Err(RejectedIo {
                request,
                reason: std::io::Error::other("注入恢复目录同步失败").into(),
            });
        }
        self.inner.submit(request)
    }
    fn poll(&self, budget: PollBudget, out: &mut Vec<IoCompletion>) -> Result<(), Error> {
        self.inner.poll(budget, out)
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.inner.shutdown(deadline)?;
        self.stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
#[test]
fn 恢复安装同步失败会排空设备且超时后可以重新恢复() {
    let (_root, store) = setup(None);
    let config = store.inner.config.clone();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    put(&mut session, 7, 19);
    let report = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    let set = crate::api::maintenance::RecoverySet {
        store: store.id(),
        index: report.token,
        log: report.token,
    };
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let result = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config.clone())
        .device(Box::new(RecoverySyncFaultFactory(stopped.clone())))
        .recover(set.clone());
    assert!(matches!(result, Err(Error::Io(_))));
    assert!(stopped.load(std::sync::atomic::Ordering::SeqCst));
    let mut short = config.clone();
    short.recovery.timeout = Duration::from_nanos(1);
    assert!(matches!(
        recover_store(short, set.clone()),
        Err(Error::DeadlineExceeded)
    ));
    let (store, _) = recover_store(config, set).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[path = "checkpoint_safety_tests.rs"]
mod safety;
