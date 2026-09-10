//! 原生缓存命中、失效及维护路径验证。
use super::*;
use crate::schema::KeyCodec;
fn cached_store(capacity: usize) -> (Directory, RasterKV<Schema>) {
    let root = Directory(std::env::temp_dir().join(format!(
        "raster-cache-{:x?}",
        StoreId::generate().unwrap().0
    )));
    let mut config = Config::default();
    config.storage.root = root.0.clone();
    config.log.page_bytes = 4096;
    config.log.memory_pages = 4;
    config.cache.enabled = true;
    config.cache.capacity_bytes = capacity;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 16,
        }))
        .create()
        .unwrap();
    (root, store)
}
fn cold_keys(store: &RasterKV<Schema>) -> Session<Schema> {
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    for key in 0..400 {
        put(&mut session, key, key);
    }
    session
}
fn hit(session: &mut Session<Schema>, serial: u64, key: u64, value: u64) {
    let Submission::Ready(result) = session
        .read(Serial(serial), Read(key), Default::default())
        .unwrap()
    else {
        panic!("缓存应同步命中")
    };
    assert!(
        matches!(result.unwrap(), crate::api::completion::Outcome::Success(actual) if actual == value)
    );
}
#[derive(Debug)]
struct Write(u64, u64);
impl Keyed<Schema> for Write {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl UpsertOperation<Schema> for Write {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.1, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[test]
fn 冷页缓存同步命中且写入删除后不会返回旧值() {
    let (_root, store) = cached_store(8192);
    let mut session = cold_keys(&store);
    assert_eq!(read_value(&mut session, 400, 0), Some(0));
    assert!(matches!(
        store.inner.index.prepare(U64Key.hash(&0)).unwrap().head,
        crate::index::IndexHead::Cache(_)
    ));
    hit(&mut session, 401, 0, 0);
    match session.upsert(Serial(402), Write(0, 999)).unwrap() {
        Submission::Ready(result) => {
            result.unwrap();
        }
        Submission::Pending(mut ticket) => {
            session.wait(&mut ticket, deadline()).unwrap().unwrap();
        }
    }
    assert_eq!(read_value(&mut session, 403, 0), Some(999));
    assert_eq!(read_value(&mut session, 404, 1), Some(1));
    hit(&mut session, 405, 1, 1);
    match session
        .delete(Serial(406), Delete(1), Default::default())
        .unwrap()
    {
        Submission::Ready(result) => {
            result.unwrap();
        }
        Submission::Pending(mut ticket) => {
            session.wait(&mut ticket, deadline()).unwrap().unwrap();
        }
    }
    assert_eq!(read_value(&mut session, 407, 1), None);
    assert!(store.inner.cache.allocated_bytes() <= 8192);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 迟到磁盘读取不将更新或删除之前的值重新装入缓存() {
    let (_root, store) = cached_store(8192);
    let mut session = cold_keys(&store);
    let Submission::Pending(mut old) = session
        .read(Serial(400), Read(0), Default::default())
        .unwrap()
    else {
        panic!("旧页应挂起")
    };
    let Submission::Ready(result) = session.upsert(Serial(401), Write(0, 999)).unwrap() else {
        panic!("尾部应有空间立即写入")
    };
    result.unwrap();
    assert!(matches!(
        session.wait(&mut old, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(0)
    ));
    assert_eq!(read_value(&mut session, 402, 0), Some(999));
    let Submission::Pending(mut old) = session
        .read(Serial(403), Read(1), Default::default())
        .unwrap()
    else {
        panic!("旧页应挂起")
    };
    let mut deleter = store.start_session(SessionOptions::default()).unwrap();
    match deleter
        .delete(Serial(0), Delete(1), Default::default())
        .unwrap()
    {
        Submission::Ready(result) => {
            result.unwrap();
        }
        Submission::Pending(mut ticket) => {
            deleter.wait(&mut ticket, deadline()).unwrap().unwrap();
        }
    }
    assert!(matches!(
        session.wait(&mut old, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(1)
    ));
    assert_eq!(read_value(&mut session, 404, 1), None);
    assert_eq!(store.inner.cache.allocated_bytes(), 0);
    deleter.close(deadline()).unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 缓存淘汰扩容检查点与恢复始终保持主日志地址() {
    let (_root, store) = cached_store(512);
    let mut config = store.inner.config.clone();
    let mut session = cold_keys(&store);
    let id = session.id();
    for key in 0..8 {
        assert_eq!(read_value(&mut session, 400 + key, key), Some(key));
    }
    assert!(matches!(
        store.inner.index.prepare(U64Key.hash(&0)).unwrap().head,
        crate::index::IndexHead::Log(_)
    ));
    hit(&mut session, 408, 7, 7);
    assert!(store.inner.cache.allocated_bytes() <= 512);
    let growth = store.maintenance().grow_index().unwrap();
    let report = session.wait_maintenance(&growth, deadline()).unwrap();
    config.index.buckets = report.as_ref().as_ref().unwrap().new_buckets;
    assert_eq!(store.inner.cache.allocated_bytes(), 0);
    assert_eq!(read_value(&mut session, 409, 0), Some(0));
    hit(&mut session, 410, 0, 0);
    let checkpoint = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    assert_eq!(store.inner.cache.allocated_bytes(), 0);
    let image = manifest(&store, &checkpoint);
    assert!(!image.materials.is_empty());
    let set = crate::api::maintenance::RecoverySet {
        store: store.id(),
        index: checkpoint.token,
        log: checkpoint.token,
    };
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, _) = recover_store(config, set).unwrap();
    assert_eq!(store.inner.cache.allocated_bytes(), 0);
    let mut session = store.continue_session(id).unwrap().session;
    let Submission::Pending(mut first) = session
        .read(Serial(411), Read(0), Default::default())
        .unwrap()
    else {
        panic!("恢复不保留缓存")
    };
    assert!(matches!(
        session.wait(&mut first, deadline()).unwrap().unwrap(),
        crate::api::completion::Outcome::Success(0)
    ));
    hit(&mut session, 412, 0, 0);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

#[derive(Debug)]
struct PausedWrite {
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}
impl Keyed<Schema> for PausedWrite {
    fn key(&self) -> &u64 {
        &0
    }
}
impl UpsertOperation<Schema> for PausedWrite {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        self.entered.send(()).unwrap();
        self.release.recv_timeout(Duration::from_secs(10)).unwrap();
        Ok((123, ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
#[test]
fn 写回调暂停时缓存安装与淘汰让出且不破坏其发布快照() {
    let (_root, store) = cached_store(200);
    let mut session = cold_keys(&store);
    assert_eq!(read_value(&mut session, 400, 0), Some(0));
    assert!(matches!(
        store.inner.index.prepare(U64Key.hash(&0)).unwrap().head,
        crate::index::IndexHead::Cache(_)
    ));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let worker_store = store.clone();
        let worker = scope.spawn(move || {
            let mut writer = worker_store
                .start_session(SessionOptions::default())
                .unwrap();
            let Submission::Ready(result) = writer
                .upsert(
                    Serial(0),
                    PausedWrite {
                        entered: entered_tx,
                        release: release_rx,
                    },
                )
                .unwrap()
            else {
                panic!("应立即发布")
            };
            result.unwrap();
            writer.close(deadline()).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(read_value(&mut session, 401, 1), Some(1));
        assert!(matches!(
            store.inner.index.prepare(U64Key.hash(&0)).unwrap().head,
            crate::index::IndexHead::Cache(_)
        ));
        assert!(matches!(
            store.inner.index.prepare(U64Key.hash(&1)).unwrap().head,
            crate::index::IndexHead::Log(_)
        ));
        release_tx.send(()).unwrap();
        worker.join().unwrap();
    });
    assert_eq!(read_value(&mut session, 402, 0), Some(123));
    assert_eq!(read_value(&mut session, 403, 1), Some(1));
    hit(&mut session, 404, 1, 1);
    assert_eq!(read_value(&mut session, 405, 0), Some(123));
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn 缓存预算不足仅跳过安装而不使正常读取失败() {
    let (_root, store) = cached_store(1);
    let mut session = cold_keys(&store);
    for serial in 400..402 {
        let Submission::Pending(mut ticket) = session
            .read(Serial(serial), Read(0), Default::default())
            .unwrap()
        else {
            panic!("预算不足时仍走磁盘")
        };
        assert!(matches!(
            session.wait(&mut ticket, deadline()).unwrap().unwrap(),
            crate::api::completion::Outcome::Success(0)
        ));
    }
    assert_eq!(store.inner.cache.allocated_bytes(), 0);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
