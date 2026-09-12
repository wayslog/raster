//! public lifecycle has run;Four operations are still P3.2 Access.
use raster::{
    RasterKV,
    api::session::SessionOptions,
    device::null::NullDeviceFactory,
    schema::builtin::{AtomicU64Value, SchemaPair, U64Key},
    types::*,
};
use std::time::{Duration, Instant};
fn deadline() -> Deadline {
    Deadline(Instant::now() + Duration::from_secs(2))
}
fn store() -> RasterKV<SchemaPair<U64Key, AtomicU64Value>> {
    RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(NullDeviceFactory))
        .create()
        .unwrap()
}
#[test]
fn create_a_shared_session_and_close_the_complete_life_cycle() {
    let store = store();
    let clone = store.clone();
    let mut session = store.start_session(SessionOptions::default()).unwrap();
    let id = session.id();
    id.validate().unwrap();
    assert_eq!(session.last_accepted(), None);
    assert!(matches!(
        clone.start_session(SessionOptions { id: Some(id) }),
        Err(Error::Busy)
    ));
    assert!(matches!(store.shutdown(deadline()), Err(Error::Busy)));
    let busy = store.diagnostics().unwrap();
    assert_eq!(busy.active_session_ids, vec![id]);
    assert_eq!(busy.active_sessions, busy.active_session_ids.len());
    assert!(session.close(deadline()).unwrap().drained);
    assert!(store.diagnostics().unwrap().active_session_ids.is_empty());
    assert!(session.close(deadline()).unwrap().drained);
    assert!(clone.shutdown(deadline()).unwrap().device_drained);
    assert!(store.shutdown(deadline()).unwrap().device_drained);
    assert!(clone.start_session(SessionOptions::default()).is_err());
}
#[test]
fn discard_the_empty_session_and_log_out_and_you_can_re_register_with_the_same_identity() {
    let store = store();
    let id = SessionId([1; 16]);
    let session = store
        .start_session(SessionOptions { id: Some(id) })
        .unwrap();
    drop(session);
    let mut reopened = store
        .start_session(SessionOptions { id: Some(id) })
        .unwrap();
    assert_eq!(reopened.id(), id);
    reopened.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn active_session_caps_and_invalid_identity_still_work_after_rejection() {
    let mut config = raster::config::Config::default();
    config.session.max_sessions = 1;
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .config(config)
        .device(Box::new(NullDeviceFactory))
        .create()
        .unwrap();
    assert!(
        store
            .start_session(SessionOptions {
                id: Some(SessionId([0; 16]))
            })
            .is_err()
    );
    let first = store.start_session(SessionOptions::default()).unwrap();
    assert!(matches!(
        store.start_session(SessionOptions::default()),
        Err(Error::CapacityExceeded)
    ));
    drop(first);
    let second = store.start_session(SessionOptions::default()).unwrap();
    drop(second);
    store.shutdown(deadline()).unwrap();
}
#[test]
fn shared_instances_can_be_registered_in_another_thread_and_the_session_closed() {
    let store = store();
    let clone = store.clone();
    std::thread::spawn(move || {
        let mut session = clone.start_session(SessionOptions::default()).unwrap();
        session.close(deadline()).unwrap();
    })
    .join()
    .unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn thread_session_quotas_are_isolated_by_instance_and_can_be_re_registered_without_destroying_the_object()
 {
    let first_store = store();
    let other_store = store();
    let clone = first_store.clone();
    let mut first = first_store.start_session(Default::default()).unwrap();
    assert!(matches!(
        clone.start_session(Default::default()),
        Err(Error::Busy)
    ));
    let mut independent = other_store.start_session(Default::default()).unwrap();
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut session = clone.start_session(Default::default()).unwrap();
                session.close(deadline()).unwrap();
            })
            .join()
            .unwrap();
    });
    first.close(deadline()).unwrap();
    let mut replacement = clone.start_session(Default::default()).unwrap();
    replacement.close(deadline()).unwrap();
    independent.close(deadline()).unwrap();
    first_store.shutdown(deadline()).unwrap();
    other_store.shutdown(deadline()).unwrap();
}
#[test]
fn session_preparation_refuses_to_return_the_thread_quota_without_affecting_the_original_session() {
    let store = store();
    assert!(
        store
            .start_session(SessionOptions {
                id: Some(SessionId([0; 16]))
            })
            .is_err()
    );
    assert!(store.continue_session(SessionId([9; 16])).is_err());
    let mut active = store.start_session(Default::default()).unwrap();
    let id = active.id();
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                assert!(matches!(
                    store.start_session(SessionOptions { id: Some(id) }),
                    Err(Error::Busy)
                ));
                let mut other = store.start_session(Default::default()).unwrap();
                other.close(deadline()).unwrap();
            })
            .join()
            .unwrap();
    });
    assert_eq!(active.id(), id);
    active.close(deadline()).unwrap();
    store.shutdown(deadline()).unwrap();
}
#[test]
fn device_shutdown_failure_cannot_report_success_and_retrying_will_not_reopen_registration() {
    use raster::device::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Factory(Arc<AtomicUsize>);
    struct DeviceState(Arc<AtomicUsize>);
    impl DeviceFactory for Factory {
        fn open(&self, _: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
            Ok(Box::new(DeviceState(self.0.clone())))
        }
    }
    impl Device for DeviceState {
        fn capabilities(&self) -> DeviceCapabilities {
            DeviceCapabilities {
                supports_files: false,
                memory_alignment: 1,
                transfer_alignment: 1,
                supports_file_sync: false,
                supports_directory_sync: false,
                supports_atomic_publish: false,
                supports_directory_listing: false,
                supports_file_locks: false,
            }
        }
        fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
            Err(RejectedIo {
                request,
                reason: Error::UnsupportedDurability,
            })
        }
        fn poll(&self, _: PollBudget, _: &mut Vec<IoCompletion>) -> Result<(), Error> {
            Ok(())
        }
        fn shutdown(&self, _: Deadline) -> Result<(), Error> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(Error::DeadlineExceeded)
            } else {
                Ok(())
            }
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(Factory(calls.clone())))
        .create()
        .unwrap();
    assert!(matches!(
        store.shutdown(deadline()),
        Err(Error::DeadlineExceeded)
    ));
    assert!(store.start_session(SessionOptions::default()).is_err());
    assert!(store.shutdown(deadline()).unwrap().device_drained);
    store.shutdown(deadline()).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
