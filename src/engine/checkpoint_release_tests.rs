//! Public release entry to verify real disk dependencies,Preserved collection defer and persistent retries.
use super::*;
use crate::api::maintenance::{CheckpointReleaseReport, PhysicalReclamation, RecoverySet};
fn checkpoint(
    store: &RasterKV<Schema>,
    session: &mut Session<Schema>,
    kind: CheckpointKind,
) -> CheckpointReport {
    wait(session, &store.maintenance().checkpoint(kind).unwrap())
}
fn release(
    store: &RasterKV<Schema>,
    session: &mut Session<Schema>,
    token: CheckpointToken,
) -> crate::api::maintenance::SharedReport<CheckpointReleaseReport> {
    session
        .wait_maintenance(
            &store.maintenance().release_checkpoint(token).unwrap(),
            deadline(),
        )
        .unwrap()
}
fn object(store: &RasterKV<Schema>, token: CheckpointToken, name: &str) -> PathBuf {
    store
        .inner
        .config
        .storage
        .root
        .join(store.inner.storage.checkpoint_path(token, name).unwrap())
}
fn set(store: &RasterKV<Schema>, checkpoint: &CheckpointReport) -> RecoverySet {
    RecoverySet {
        store: store.id(),
        index: checkpoint.token,
        log: checkpoint.token,
    }
}
#[test]
fn releasing_the_old_full_checkpoint_deletes_only_its_material_and_retains_the_new_collection_and_active_data()
 {
    let (_root, store) = setup(None);
    let config = store.inner.config.clone();
    let mut session = store.start_session(Default::default()).unwrap();
    for key in 0..100 {
        put(&mut session, key, key);
    }
    let old = checkpoint(&store, &mut session, CheckpointKind::Full);
    let old_manifest = manifest(&store, &old);
    let old_set = set(&store, &old);
    put(&mut session, 100, 100);
    let current = checkpoint(&store, &mut session, CheckpointKind::Full);
    let current_set = set(&store, &current);
    let result = release(&store, &mut session, old.token);
    let report = result.as_ref().as_ref().unwrap();
    assert_eq!(report.retirement, CheckpointRetirement::Retired);
    assert_eq!(
        report.confirmed_absent_materials,
        old_manifest.materials.len() as u64
    );
    assert!(matches!(report.physical, PhysicalReclamation::Completed));
    assert!(!object(&store, old.token, "commit").exists());
    for name in ["commit.released", "manifest", "owner"] {
        assert!(object(&store, old.token, name).exists());
    }
    for material in &old_manifest.materials {
        assert!(
            !object(
                &store,
                old.token,
                &crate::storage::SegmentedStorage::checkpoint_material_name(
                    material.id,
                    material.generation
                )
            )
            .exists()
        );
    }
    assert!(object(&store, current.token, "commit").exists());
    assert_eq!(read_value(&mut session, 101, 100), Some(100));
    let result = release(&store, &mut session, old.token);
    assert_eq!(
        result.as_ref().as_ref().unwrap().confirmed_absent_materials,
        old_manifest.materials.len() as u64
    );
    assert!(recover_store(config.clone(), old_set).is_err());
    let (recovered, _) = recover_store(config, current_set).unwrap();
    let mut reader = recovered.start_session(Default::default()).unwrap();
    assert_eq!(read_value(&mut reader, 0, 100), Some(100));
    reader.close(deadline()).unwrap();
    recovered.shutdown(deadline()).unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn two_log_references_in_the_disk_make_the_old_index_deferred_and_can_be_reclaimed_after_the_references_are_released()
 {
    let (_root, store) = setup(None);
    let config = store.inner.config.clone();
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let base = checkpoint(&store, &mut session, CheckpointKind::Index);
    let log1 = checkpoint(&store, &mut session, CheckpointKind::Log);
    put(&mut session, 1, 8);
    let log2 = checkpoint(&store, &mut session, CheckpointKind::Log);
    let newest = checkpoint(&store, &mut session, CheckpointKind::Full);
    let newest_set = set(&store, &newest);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
    drop(store);
    let (store, _) = recover_store(config.clone(), newest_set.clone()).unwrap();
    assert!(
        !store
            .inner
            .checkpoints
            .lock()
            .unwrap()
            .retained
            .references_token(base.token),
        "Restoring instance cache doesn't know about old references"
    );
    let mut session = store.start_session(Default::default()).unwrap();
    let result = release(&store, &mut session, base.token);
    let report = result.as_ref().as_ref().unwrap();
    assert_eq!(report.retirement, CheckpointRetirement::NotAttempted);
    assert_eq!(report.confirmed_absent_materials, 0);
    let PhysicalReclamation::DeferredByRecoverySet { blockers, .. } = &report.physical else {
        panic!("Should be postponed by recovery set")
    };
    let actual: std::collections::BTreeSet<_> = blockers
        .iter()
        .map(|set| {
            assert_eq!(set.store, store.id());
            assert_eq!(set.index, base.token);
            set.log
        })
        .collect();
    assert_eq!(actual, [log1.token, log2.token].into_iter().collect());
    assert!(object(&store, base.token, "commit").exists());
    // Delayed reporting cannot be justified solely by the fact that the documents are still available:Both protected collections must actually be restored.
    for (log, expected) in [(log1.token, None), (log2.token, Some(8))] {
        let (reader, _) = recover_store(
            config.clone(),
            RecoverySet {
                store: store.id(),
                index: base.token,
                log,
            },
        )
        .unwrap();
        let mut read = reader.start_session(Default::default()).unwrap();
        assert_eq!(read_value(&mut read, 0, 7), Some(7));
        assert_eq!(read_value(&mut read, 1, 8), expected);
        read.close(deadline()).unwrap();
        reader.shutdown(deadline()).unwrap();
    }
    // Delay the end of this action,It is still possible to checkpoint and release dependencies explicitly later.
    checkpoint(&store, &mut session, CheckpointKind::Full);
    release(&store, &mut session, log1.token)
        .as_ref()
        .as_ref()
        .unwrap();
    let result = release(&store, &mut session, base.token);
    assert!(
        matches!(&result.as_ref().as_ref().unwrap().physical, PhysicalReclamation::DeferredByRecoverySet { blockers, .. } if blockers.len()==1 && blockers[0].log==log2.token)
    );
    let (reader, _) = recover_store(
        config.clone(),
        RecoverySet {
            store: store.id(),
            index: base.token,
            log: log2.token,
        },
    )
    .unwrap();
    let mut read = reader.start_session(Default::default()).unwrap();
    assert_eq!(read_value(&mut read, 0, 8), Some(8));
    read.close(deadline()).unwrap();
    reader.shutdown(deadline()).unwrap();
    release(&store, &mut session, log2.token)
        .as_ref()
        .as_ref()
        .unwrap();
    let result = release(&store, &mut session, base.token);
    assert_eq!(
        result.as_ref().as_ref().unwrap().retirement,
        CheckpointRetirement::Retired
    );
    let (reader, _) = recover_store(config, newest_set).unwrap();
    reader.shutdown(deadline()).unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn directory_lock_competition_terminates_this_release_and_does_not_automatically_expire_before_explicitly_retrying()
 {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let old = checkpoint(&store, &mut session, CheckpointKind::Full);
    let guard = std::fs::File::options()
        .read(true)
        .write(true)
        .open(store.inner.config.storage.root.join("checkpoint.lock"))
        .unwrap();
    guard.try_lock_shared().unwrap();
    let result = release(&store, &mut session, old.token);
    assert!(
        matches!(&*result, Err(Error::CheckpointReleaseFailed { retirement: CheckpointRetirement::NotAttempted, cause, .. }) if matches!(&**cause, Error::Busy))
    );
    assert!(!store.inner.failed.load(std::sync::atomic::Ordering::SeqCst));
    checkpoint(&store, &mut session, CheckpointKind::Full);
    guard.unlock().unwrap();
    for _ in 0..16 {
        store.maintenance().poll(PollBudget::default()).unwrap();
    }
    assert!(object(&store, old.token, "commit").exists());
    release(&store, &mut session, old.token)
        .as_ref()
        .as_ref()
        .unwrap();
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

fn refused(result: &crate::api::maintenance::SharedReport<CheckpointReleaseReport>) {
    assert!(
        matches!(&**result, Err(Error::CheckpointReleaseFailed { retirement: CheckpointRetirement::NotAttempted, confirmed_absent_materials: 0, cause, .. }) if matches!(&**cause, Error::InvalidFormat(_) | Error::CapacityExceeded))
    );
}
#[test]
fn unknown_directory_corruption_list_and_missing_material_description_cannot_authorize_deletion_of_other_checkpoints()
 {
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    put(&mut session, 0, 7);
    let target = checkpoint(&store, &mut session, CheckpointKind::Full);
    let other = checkpoint(&store, &mut session, CheckpointKind::Full);
    let target_commit = object(&store, target.token, "commit");
    let unknown = store
        .inner
        .config
        .storage
        .root
        .join("checkpoints/unknown directory");
    std::fs::create_dir(&unknown).unwrap();
    std::fs::write(unknown.join("Keep files"), b"keep").unwrap();
    refused(&release(&store, &mut session, target.token));
    assert!(target_commit.exists());
    assert!(unknown.join("Keep files").exists());
    std::fs::remove_dir_all(&unknown).unwrap();
    let path = object(&store, other.token, "manifest");
    let bytes = std::fs::read(&path).unwrap();
    let mut corrupt = bytes.clone();
    corrupt[0] ^= 0xff;
    std::fs::write(&path, corrupt).unwrap();
    refused(&release(&store, &mut session, target.token));
    assert!(target_commit.exists());
    std::fs::remove_file(&path).unwrap();
    refused(&release(&store, &mut session, target.token));
    assert!(target_commit.exists());
    std::fs::write(&path, bytes).unwrap();
    let released = object(&store, other.token, "commit.released");
    std::fs::copy(object(&store, other.token, "commit"), &released).unwrap();
    refused(&release(&store, &mut session, target.token));
    assert!(target_commit.exists());
    std::fs::remove_file(released).unwrap();
    let link = store
        .inner
        .config
        .storage
        .root
        .join("checkpoints")
        .join("ab".repeat(16));
    std::os::unix::fs::symlink(path.parent().unwrap(), &link).unwrap();
    refused(&release(&store, &mut session, target.token));
    assert!(target_commit.exists());
    std::fs::remove_file(link).unwrap();
    assert!(!store.inner.failed.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(read_value(&mut session, 1, 7), Some(7));
    release(&store, &mut session, target.token)
        .as_ref()
        .as_ref()
        .unwrap();
    checkpoint(&store, &mut session, CheckpointKind::Full);
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn catalog_and_metadata_budget_exceeded_overall_rejection_and_unsubmitted_catalog_retained_by_default()
 {
    for (tokens, bytes) in [(1, 64 * 1024 * 1024), (4096, 1), (4096, 512)] {
        let root = Directory(std::env::temp_dir().join(format!(
            "raster-release-budget-{:x?}",
            StoreId::generate().unwrap().0
        )));
        let mut config = Config::default();
        config.storage.root = root.0.clone();
        config.log.page_bytes = 4096;
        config.maintenance.max_checkpoint_tokens = tokens;
        config.maintenance.max_checkpoint_catalog_bytes = bytes;
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .config(config)
            .device(Box::new(device::thread_pool::ThreadPoolDeviceFactory {
                workers: 2,
                queue_capacity: 16,
            }))
            .create()
            .unwrap();
        let mut session = store.start_session(Default::default()).unwrap();
        put(&mut session, 0, 7);
        let target = checkpoint(&store, &mut session, CheckpointKind::Full);
        checkpoint(&store, &mut session, CheckpointKind::Full);
        refused(&release(&store, &mut session, target.token));
        assert!(object(&store, target.token, "commit").exists());
        assert!(!store.inner.failed.load(std::sync::atomic::Ordering::SeqCst));
        session.close(deadline()).unwrap();
        store.shutdown(deadline()).unwrap();
    }
    let (_root, store) = setup(None);
    let mut session = store.start_session(Default::default()).unwrap();
    let target = checkpoint(&store, &mut session, CheckpointKind::Full);
    let orphan = CheckpointToken::generate().unwrap();
    let owner = object(&store, orphan, "owner");
    std::fs::create_dir(owner.parent().unwrap()).unwrap();
    std::fs::write(&owner, b"incomplete").unwrap();
    release(&store, &mut session, target.token)
        .as_ref()
        .as_ref()
        .unwrap();
    assert!(owner.exists());
    refused(&release(&store, &mut session, orphan));
    assert!(owner.exists());
    session.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}

mod failure {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };
    #[derive(Clone, Copy, Debug)]
    enum Point {
        Rename,
        RetirementSync,
        Remove,
        DeleteSync,
    }
    struct Control {
        point: Point,
        before: bool,
        armed: AtomicBool,
        directory: Mutex<Option<PathBuf>>,
        pending: Mutex<std::collections::BTreeSet<IoId>>,
        counts: Mutex<[usize; 3]>,
    }
    struct Factory(Arc<Control>);
    struct Fault {
        inner: Box<dyn Device>,
        control: Arc<Control>,
    }
    impl DeviceFactory for Factory {
        fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
            Ok(Box::new(Fault {
                inner: device::thread_pool::ThreadPoolDeviceFactory {
                    workers: 2,
                    queue_capacity: 16,
                }
                .open(options)?,
                control: self.0.clone(),
            }))
        }
    }
    impl Device for Fault {
        fn capabilities(&self) -> DeviceCapabilities {
            self.inner.capabilities()
        }
        fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
            let target = self.control.directory.lock().unwrap().clone();
            let event = target.as_ref().and_then(|target| match &request.operation {
                IoOperation::Rename {
                    source,
                    destination,
                } if source == &target.join("commit")
                    && destination == &target.join("commit.released") =>
                {
                    Some(0)
                }
                IoOperation::RemoveFile(path) if path.parent() == Some(target.as_path()) => Some(1),
                IoOperation::SyncDirectory(path) if path == target => Some(2),
                _ => None,
            });
            let selected = if let Some(event) = event {
                let mut counts = self.control.counts.lock().unwrap();
                counts[event] += 1;
                match self.control.point {
                    Point::Rename => event == 0,
                    Point::RetirementSync => event == 2 && counts[2] == 1,
                    Point::Remove => event == 1,
                    Point::DeleteSync => event == 2 && counts[2] == 2,
                }
            } else {
                false
            };
            let inject = selected && self.control.armed.swap(false, Ordering::SeqCst);
            if inject && self.control.before {
                return Err(RejectedIo {
                    request,
                    reason: Error::Io(std::io::Error::other(
                        "Injection release operation failed before acceptance",
                    )),
                });
            }
            let id = self.inner.submit(request)?;
            if inject {
                self.control.pending.lock().unwrap().insert(id);
            }
            Ok(id)
        }
        fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
            self.inner.poll(budget, output)?;
            for completion in output {
                if self.control.pending.lock().unwrap().remove(&completion.id) {
                    assert!(matches!(completion.result, Ok(IoOutcome::Done)));
                    completion.result = Err(Error::Io(std::io::Error::other(
                        "Release operation has been performed,Completion report failed",
                    )));
                }
            }
            Ok(())
        }
        fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
            self.inner.shutdown(deadline)
        }
    }
    #[test]
    fn invalid_deletions_and_pre_and_post_synchronization_failures_retain_the_true_impact_and_are_only_retried_by_explicit_actions()
     {
        for point in [
            Point::Rename,
            Point::RetirementSync,
            Point::Remove,
            Point::DeleteSync,
        ] {
            for before in [true, false] {
                let control = Arc::new(Control {
                    point,
                    before,
                    armed: false.into(),
                    directory: Mutex::new(None),
                    pending: Mutex::new(Default::default()),
                    counts: Mutex::new([0; 3]),
                });
                let (_root, store) = setup(Some(Box::new(Factory(control.clone()))));
                let config = store.inner.config.clone();
                let mut session = store.start_session(Default::default()).unwrap();
                put(&mut session, 0, 7);
                let old = checkpoint(&store, &mut session, CheckpointKind::Full);
                let old_manifest = manifest(&store, &old);
                let latest = checkpoint(&store, &mut session, CheckpointKind::Full);
                let latest_set = set(&store, &latest);
                let directory = store
                    .inner
                    .storage
                    .checkpoint_path(old.token, "commit")
                    .unwrap()
                    .parent()
                    .unwrap()
                    .to_path_buf();
                *control.directory.lock().unwrap() = Some(directory);
                control.armed.store(true, Ordering::SeqCst);
                let result = release(&store, &mut session, old.token);
                let expected = match (point, before) {
                    (Point::Rename, true) => CheckpointRetirement::NotAttempted,
                    (Point::Rename | Point::RetirementSync, _) => {
                        CheckpointRetirement::PossiblyRetired
                    }
                    _ => CheckpointRetirement::Retired,
                };
                assert!(
                    matches!(&*result, Err(Error::CheckpointReleaseFailed { retirement, confirmed_absent_materials: 0, cause, .. }) if *retirement == expected && matches!(&**cause, Error::Io(_))),
                    "{point:?} / {before}: {result:?}"
                );
                let counts = *control.counts.lock().unwrap();
                for _ in 0..16 {
                    store.maintenance().poll(PollBudget::default()).unwrap();
                }
                assert_eq!(
                    *control.counts.lock().unwrap(),
                    counts,
                    "Failure will not automatically retry"
                );
                assert!(!store.inner.failed.load(Ordering::SeqCst));
                if matches!(point, Point::Rename | Point::RetirementSync) {
                    assert_eq!(
                        counts[1], 0,
                        "Material cannot be deleted before invalidation is confirmed"
                    );
                }
                checkpoint(&store, &mut session, CheckpointKind::Full);
                let result = release(&store, &mut session, old.token);
                let report = result.as_ref().as_ref().unwrap();
                assert_eq!(report.retirement, CheckpointRetirement::Retired);
                assert_eq!(
                    report.confirmed_absent_materials,
                    old_manifest.materials.len() as u64
                );
                assert!(!object(&store, old.token, "commit").exists());
                let (reader, _) = recover_store(config, latest_set).unwrap();
                reader.shutdown(deadline()).unwrap();
                session.close(deadline()).unwrap();
                store.shutdown(deadline()).unwrap();
            }
        }
    }
}

#[path = "checkpoint_release_crash_tests.rs"]
mod crash;
