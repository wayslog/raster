//! 公开内存 Read/Upsert 路径；RMW/Delete 另行接入。
use raster::{
    RasterKV, Submission,
    api::{completion::Outcome, operation::*, session::SessionOptions},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::sync::atomic::Ordering;
type Schema = SchemaPair<U64Key, AtomicU64Value>;
struct Read(u64);
impl Keyed<Schema> for Read {
    fn key(&self) -> &u64 {
        &self.0
    }
}
impl ReadOperation<Schema> for Read {
    type Output = u64;
    fn read(&mut self, v: ValueRead<'_, Schema>) -> Result<u64, Error> {
        Ok(*v.view())
    }
}
struct Put {
    key: u64,
    value: u64,
    append: bool,
    fail: bool,
}
impl Keyed<Schema> for Put {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl UpsertOperation<Schema> for Put {
    type Output = u64;
    fn replacement(&mut self) -> Result<(u64, u64), Error> {
        Ok((self.value, self.value))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<u64>, Error> {
        if self.append {
            return Ok(UpdateDecision::Append);
        }
        v.view_mut().store(self.value, Ordering::SeqCst);
        if self.fail {
            return Err(Error::Codec("修改后故障"));
        }
        Ok(UpdateDecision::Updated(self.value))
    }
}
fn put(key: u64, value: u64) -> Put {
    Put {
        key,
        value,
        append: false,
        fail: false,
    }
}
fn store() -> RasterKV<Schema> {
    RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
        .device(Box::new(raster::device::null::NullDeviceFactory))
        .create()
        .unwrap()
}
fn outcome<T: 'static>(s: Submission<T>) -> Outcome<T> {
    match s {
        Submission::Ready(Ok(o)) => o,
        _ => panic!("预期同步业务结果"),
    }
}
#[test]
fn 空存储插入原地更新与追加替换可读() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    assert!(matches!(
        outcome(
            s.read(Serial(0), Read(7), ReadOptions::default())
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::NotFound
    ));
    for (serial, value, append) in [(1, 10, false), (3, 20, false), (5, 30, true)] {
        let mut p = put(7, value);
        p.append = append;
        assert!(
            matches!(outcome(s.upsert(Serial(serial),p).map_err(|r|r.reason).unwrap()),Outcome::Success(v) if v==value)
        );
        assert!(
            matches!(outcome(s.read(Serial(serial+1),Read(7),ReadOptions::default()).map_err(|r|r.reason).unwrap()),Outcome::Success(v) if v==value)
        );
    }
    assert!(s.upsert(Serial(6), put(7, 99)).is_err());
    assert_eq!(s.last_accepted(), Some(Serial(6)));
    assert!(matches!(
        outcome(
            s.read(Serial(8), Read(7), ReadOptions::default())
                .map_err(|r| r.reason)
                .unwrap()
        ),
        Outcome::Success(30)
    ));
}
#[test]
fn 可能修改的错误失败关闭且不重复执行() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    s.upsert(Serial(1), put(1, 1))
        .map_err(|r| r.reason)
        .unwrap();
    let mut p = put(1, 2);
    p.fail = true;
    assert!(matches!(
        s.upsert(Serial(2), p).map_err(|r| r.reason).unwrap(),
        Submission::Ready(Err(OperationError {
            effect: Effect::Unknown,
            ..
        }))
    ));
    assert!(s.upsert(Serial(3), put(1, 3)).is_err());
    assert_eq!(s.last_accepted(), Some(Serial(2)));
    let mut second = store.start_session(SessionOptions::default()).unwrap();
    assert!(
        second
            .read(Serial(1), Read(1), ReadOptions::default())
            .is_err()
    );
    assert_eq!(second.last_accepted(), None);
}
#[test]
fn 同标签不同键通过公开接口分别读取() {
    let store = store();
    let mut s = store.start_session(SessionOptions::default()).unwrap();
    s.upsert(Serial(1), put(8969, 17))
        .map_err(|r| r.reason)
        .unwrap();
    s.upsert(Serial(2), put(9239, 29))
        .map_err(|r| r.reason)
        .unwrap();
    for (serial, key, expected) in [(3, 8969, 17), (4, 9239, 29)] {
        assert!(
            matches!(outcome(s.read(Serial(serial),Read(key),ReadOptions::default()).map_err(|r|r.reason).unwrap()),Outcome::Success(v) if v==expected)
        );
    }
}
