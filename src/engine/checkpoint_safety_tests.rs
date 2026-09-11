//! 旧恢复集与后续原地修改的隔离验证。
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct Replace {
    key: u64,
    value: u64,
    updates: Arc<AtomicUsize>,
}
impl Keyed<Schema> for Replace {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl UpsertOperation<Schema> for Replace {
    type Output = ();
    fn replacement(&mut self) -> Result<(u64, ()), Error> {
        Ok((self.value, ()))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<()>, Error> {
        value.view_mut().store(self.value, Ordering::SeqCst);
        self.updates.fetch_add(1, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(()))
    }
}
fn replace(session: &mut Session<Schema>, serial: u64, value: u64, updates: &Arc<AtomicUsize>) {
    match session
        .upsert(
            Serial(serial),
            Replace {
                key: 0,
                value,
                updates: updates.clone(),
            },
        )
        .unwrap()
    {
        Submission::Ready(result) => {
            result.unwrap();
        }
        Submission::Pending(mut ticket) => {
            session.wait(&mut ticket, deadline()).unwrap().unwrap();
        }
    }
}
fn material_bytes(store: &RasterKV<Schema>, report: &CheckpointReport) -> Vec<Vec<u8>> {
    let manifest = manifest(store, report);
    manifest
        .materials
        .iter()
        .map(|material| {
            let name = crate::storage::SegmentedStorage::checkpoint_material_name(
                material.id,
                material.generation,
            );
            let path = store
                .inner
                .storage
                .checkpoint_path(report.token, &name)
                .unwrap();
            std::fs::read(store.inner.config.storage.root.join(path)).unwrap()
        })
        .collect()
}

#[test]
fn 两代恢复集保留不同键值与墓碑且后续原地更新不改写材料() {
    let (_root, store) = setup(None);
    let config = store.inner.config.clone();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let id = session.id();
    put(&mut session, 10, 0);
    put(&mut session, 30, 1);
    let ticket = store
        .maintenance()
        .checkpoint(CheckpointKind::Full)
        .unwrap();
    let first = wait(&mut session, &ticket);
    let saved = material_bytes(&store, &first);
    let updates = Arc::new(AtomicUsize::new(0));
    replace(&mut session, 40, 2, &updates);
    replace(&mut session, 50, 3, &updates);
    assert_eq!(
        updates.load(Ordering::SeqCst),
        1,
        "确认实际执行一次原地修改"
    );
    match session
        .delete(Serial(60), Delete(1), Default::default())
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
    let second = wait(&mut session, &ticket);
    let second_saved = material_bytes(&store, &second);
    replace(&mut session, 70, 4, &updates);
    replace(&mut session, 90, 5, &updates);
    assert_eq!(updates.load(Ordering::SeqCst), 2);
    assert_eq!(material_bytes(&store, &first), saved);
    assert_eq!(material_bytes(&store, &second), second_saved);
    let store_id = store.id();
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    // 删去已经关闭的工作段，确认两代恢复只依赖各自材料副本。
    std::fs::remove_dir_all(config.storage.root.join("segments")).unwrap();
    for (report, value, other, cut) in [(first, 0, Some(1), 30), (second, 3, None, 60)] {
        let set = crate::api::maintenance::RecoverySet {
            store: store_id,
            index: report.token,
            log: report.token,
        };
        let (restored, result) = recover_store(config.clone(), set).unwrap();
        assert_eq!(result.sessions[0].serial, Serial(cut));
        let mut session = restored.continue_session(id).unwrap().session;
        assert_eq!(read_value(&mut session, 100, 0), Some(value));
        assert_eq!(read_value(&mut session, 110, 1), other);
        session.close(deadline()).unwrap();
        drop(session);
        restored.shutdown(deadline()).unwrap();
    }
}

#[path = "checkpoint_crash_tests.rs"]
mod crash;

#[test]
fn 已发布检查点保留全部已知引用且恢复重建所选集合目录() {
    let (_root, store) = setup(None);
    let config = store.inner.config.clone();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    put(&mut session, 10, 7);
    let full = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Full)
            .unwrap(),
    );
    let index = wait(
        &mut session,
        &store
            .maintenance()
            .checkpoint(CheckpointKind::Index)
            .unwrap(),
    );
    put(&mut session, 30, 9);
    let log = wait(
        &mut session,
        &store.maintenance().checkpoint(CheckpointKind::Log).unwrap(),
    );
    {
        let runtime = store.inner.checkpoints.lock().unwrap();
        assert_eq!(runtime.retained.records().count(), 3);
        for report in [&full, &index, &log] {
            let record = runtime
                .retained
                .records()
                .find(|record| record.token == report.token)
                .unwrap();
            let manifest = manifest(&store, report);
            assert_eq!(record.store, store.id());
            assert_eq!(record.base_index, manifest.base_index);
            assert_eq!(record.material_count, manifest.materials.len());
            assert_eq!(
                record.material_bytes,
                manifest.materials.iter().map(|m| m.bytes).sum::<u64>()
            );
            assert_eq!((record.begin, record.end), (report.begin, report.end));
            assert!(runtime.retained.references_token(report.token));
        }
    }
    let first_path = store.inner.config.storage.root.join(
        store
            .inner
            .storage
            .checkpoint_path(full.token, "commit")
            .unwrap(),
    );
    let set = crate::api::maintenance::RecoverySet {
        store: store.id(),
        index: index.token,
        log: log.token,
    };
    session.close(deadline()).unwrap();
    drop(session);
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, _) = recover_store(config, set).unwrap();
    let runtime = store.inner.checkpoints.lock().unwrap();
    assert_eq!(runtime.retained.records().count(), 2);
    assert!(runtime.retained.references_token(index.token));
    assert!(runtime.retained.references_token(log.token));
    assert!(!runtime.retained.references_token(full.token));
    assert!(
        first_path.exists(),
        "未知的旧代仍默认保留，不能按运行期目录裁剪磁盘"
    );
    drop(runtime);
    store.shutdown(deadline()).unwrap();
}
