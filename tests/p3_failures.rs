//! 接受前拒绝、回调/析构恐慌和错误影响的公开边界。
use raster::{
    RasterKV, Submission,
    api::{operation::*, session::SessionOptions},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::{cell::Cell, rc::Rc};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Request {
    key: u64,
    keys: Rc<Cell<usize>>,
    panic_key: bool,
    panic_drop: bool,
}
impl Keyed<Schema> for Request {
    fn key(&self) -> &u64 {
        self.keys.set(self.keys.get() + 1);
        assert!(!self.panic_key, "键恐慌");
        &self.key
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        assert!(!self.panic_drop, "析构恐慌");
    }
}
impl ReadOperation<Schema> for Request {
    type Output = u64;
    fn read(&mut self, v: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*v.view())
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        Ok((9, 9))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        Ok(UpdateDecision::Append)
    }
}
fn request() -> Request {
    Request {
        key: 1,
        keys: Rc::new(Cell::new(0)),
        panic_key: false,
        panic_drop: false,
    }
}
fn store() -> RasterKV<Schema> {
    RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()
        .unwrap()
}
#[test]
fn 序号拒绝不调用键方法且归还原请求() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    s.read(Serial(7), request(), ReadOptions::default())
        .map_err(|r| r.reason)
        .unwrap();
    let r = request();
    let counter = r.keys.clone();
    let rejected = match s.read(Serial(7), r, ReadOptions::default()) {
        Err(r) => r,
        Ok(_) => panic!("重复序号应拒绝"),
    };
    assert_eq!(counter.get(), 0);
    assert!(Rc::ptr_eq(&counter, &rejected.request.keys));
    assert_eq!(s.last_accepted(), Some(Serial(7)));
}
#[test]
fn 键恐慌拒绝不消费序号并关闭引擎() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    let mut r = request();
    r.panic_key = true;
    assert!(s.read(Serial(1), r, ReadOptions::default()).is_err());
    assert_eq!(s.last_accepted(), None);
    assert!(store.start_session(SessionOptions::default()).is_err());
}
#[test]
fn 生效后请求析构恐慌返回已生效且不展开到调用者() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    let mut r = request();
    r.panic_drop = true;
    assert!(matches!(
        s.upsert(Serial(1), r).map_err(|r| r.reason).unwrap(),
        Submission::Ready(Err(OperationError {
            effect: Effect::Applied,
            ..
        }))
    ));
    assert_eq!(s.last_accepted(), Some(Serial(1)));
    assert!(store.start_session(SessionOptions::default()).is_err());
}
