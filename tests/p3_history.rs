//! 有界线性化检查：真实调用区间加业务结果，独立枚举合法顺序。
use raster::{
    RasterKV, Submission,
    api::{completion::Outcome, operation::*, session::SessionOptions},
    schema::builtin::{AtomicU64Value, SchemaPair, U64Key},
    types::*,
};
use std::sync::{
    Barrier, Mutex,
    atomic::{AtomicU64, Ordering},
};
#[path = "support/history.rs"]
mod history;
use history::{Action, Event, Reply, Request};
fn linearizable(events: &[Event]) -> bool {
    history::linearizable_from(events, None)
}
#[test]
fn 检查器接受合法重叠但拒绝错误结果及实时顺序违例() {
    let first = Event {
        start: 0,
        end: 3,
        action: Action::Put(1, false),
        reply: Reply::Value(1),
    };
    let read = Event {
        start: 1,
        end: 2,
        action: Action::Read,
        reply: Reply::Value(1),
    };
    assert!(linearizable(&[first, read]));
    assert!(!linearizable(&[
        first,
        Event {
            reply: Reply::Value(2),
            ..read
        }
    ]));
    assert!(!linearizable(&[
        first,
        Event {
            start: 4,
            end: 5,
            reply: Reply::Missing,
            ..read
        }
    ]));
}
#[test]
fn 三会话四操作混合历史可线性化() {
    for round in 0..32 {
        let store = RasterKV::builder(SchemaPair::new(U64Key, AtomicU64Value))
            .device(Box::new(raster::device::null::NullDeviceFactory))
            .create()
            .unwrap();
        let clock = AtomicU64::new(0);
        let history = Mutex::new(Vec::new());
        let barrier = Barrier::new(3);
        let streams = [
            [
                Action::Put(round, false),
                Action::Add(1, true),
                Action::Read,
            ],
            [Action::Add(2, false), Action::Delete, Action::Read],
            [Action::Read, Action::Put(9, true), Action::Add(3, false)],
        ];
        std::thread::scope(|scope| {
            for stream in streams {
                let store = &store;
                let clock = &clock;
                let history = &history;
                let barrier = &barrier;
                scope.spawn(move || {
                    let mut session = store.start_session(SessionOptions::default()).unwrap();
                    barrier.wait();
                    for (serial, action) in stream.into_iter().enumerate() {
                        loop {
                            let start = clock.fetch_add(1, Ordering::SeqCst);
                            let request = Request(action);
                            let result = match action {
                                Action::Read => session.read(
                                    Serial(serial as u64),
                                    request,
                                    ReadOptions::default(),
                                ),
                                Action::Put(..) => session.upsert(Serial(serial as u64), request),
                                Action::Add(..) => session.rmw(
                                    Serial(serial as u64),
                                    request,
                                    RmwOptions::default(),
                                ),
                                Action::Delete => session.delete(
                                    Serial(serial as u64),
                                    request,
                                    DeleteOptions::default(),
                                ),
                            };
                            let end = clock.fetch_add(1, Ordering::SeqCst);
                            match result {
                                Err(rejected) => {
                                    assert!(matches!(rejected.reason, Error::Busy));
                                    assert_eq!(
                                        session.last_accepted(),
                                        serial.checked_sub(1).map(|s| Serial(s as u64))
                                    );
                                    std::thread::yield_now();
                                }
                                Ok(result) => {
                                    let reply = match result {
                                        Submission::Ready(Ok(Outcome::Success(reply))) => reply,
                                        Submission::Ready(Ok(Outcome::NotFound)) => Reply::Missing,
                                        _ => panic!("意外的终结结果"),
                                    };
                                    history.lock().unwrap().push(Event {
                                        start,
                                        end,
                                        action,
                                        reply,
                                    });
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        let events = history.into_inner().unwrap();
        assert_eq!(events.len(), 9);
        assert!(linearizable(&events), "轮次 {round} 历史 {events:?}");
    }
}
