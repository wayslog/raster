//! 公开生命周期已运行；四操作仍在 P3.2 接入。
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
fn 创建共享会话与关闭完整生命周期() {
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
    assert!(session.close(deadline()).unwrap().drained);
    assert!(session.close(deadline()).unwrap().drained);
    assert!(clone.shutdown(deadline()).unwrap().device_drained);
    assert!(store.shutdown(deadline()).unwrap().device_drained);
    assert!(clone.start_session(SessionOptions::default()).is_err());
}
#[test]
fn 丢弃空会话注销且同身份可重新登记() {
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
fn 活跃会话上限和无效身份拒绝后仍可工作() {
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
fn 共享实例可在另一线程注册并关闭会话() {
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
fn 设备关闭失败不能报告成功且重试不会重新开放注册() {
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
