//! 有界线性化检查：真实调用区间加业务结果，独立枚举合法顺序。
use raster::{
    RasterKV, Submission,
    api::{completion::Outcome, operation::*, session::SessionOptions},
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::*,
};
use std::sync::{
    Barrier, Mutex,
    atomic::{AtomicU64, Ordering},
};
type Schema = SchemaPair<U64Key, AtomicU64Value>;
#[derive(Clone, Copy, Debug)]
enum Action {
    Read,
    Put(u64, bool),
    Add(u64, bool),
    Delete,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reply {
    Value(u64),
    Deleted,
    Missing,
}
#[derive(Clone, Copy, Debug)]
struct Event {
    start: u64,
    end: u64,
    action: Action,
    reply: Reply,
}
struct Request(Action);
impl Keyed<Schema> for Request {
    fn key(&self) -> &u64 {
        &1
    }
}
impl ReadOperation<Schema> for Request {
    type Output = Reply;
    fn read(&mut self, v: ValueRead<'_, Schema>) -> Result<Reply, Error> {
        Ok(Reply::Value(*v.view()))
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = Reply;
    fn replacement(&mut self) -> Result<(u64, Reply), Error> {
        let Action::Put(n, _) = self.0 else {
            unreachable!()
        };
        Ok((n, Reply::Value(n)))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<Reply>, Error> {
        let Action::Put(n, copy) = self.0 else {
            unreachable!()
        };
        if copy {
            return Ok(UpdateDecision::Append);
        }
        v.view_mut().store(n, Ordering::SeqCst);
        Ok(UpdateDecision::Updated(Reply::Value(n)))
    }
}
impl RmwOperation<Schema> for Request {
    type Output = Reply;
    fn initial(&mut self) -> Result<(u64, Reply), Error> {
        let Action::Add(n, _) = self.0 else {
            unreachable!()
        };
        Ok((n, Reply::Value(n)))
    }
    fn copy_update(&mut self, v: ValueRead<'_, Schema>) -> Result<(u64, Reply), Error> {
        let Action::Add(n, _) = self.0 else {
            unreachable!()
        };
        let result = v.view().wrapping_add(n);
        Ok((result, Reply::Value(result)))
    }
    fn update_in_place(
        &mut self,
        mut v: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<Reply>, Error> {
        let Action::Add(n, copy) = self.0 else {
            unreachable!()
        };
        if copy {
            return Ok(UpdateDecision::Append);
        }
        Ok(UpdateDecision::Updated(Reply::Value(
            v.view_mut().fetch_add(n, Ordering::SeqCst).wrapping_add(n),
        )))
    }
}
impl DeleteOperation<Schema> for Request {
    type Output = Reply;
    fn complete(self, _: DeleteOutcome) -> Reply {
        Reply::Deleted
    }
}
fn transition(state: Option<u64>, action: Action) -> (Option<u64>, Reply) {
    match action {
        Action::Read => (state, state.map_or(Reply::Missing, Reply::Value)),
        Action::Put(n, _) => (Some(n), Reply::Value(n)),
        Action::Add(n, _) => {
            let n = state.map_or(n, |v| v.wrapping_add(n));
            (Some(n), Reply::Value(n))
        }
        Action::Delete => (
            None,
            if state.is_some() {
                Reply::Deleted
            } else {
                Reply::Missing
            },
        ),
    }
}
fn linearizable(events: &[Event]) -> bool {
    fn search(events: &[Event], used: u64, state: Option<u64>) -> bool {
        if used == (1 << events.len()) - 1 {
            return true;
        }
        for (i, event) in events.iter().enumerate() {
            if used & (1 << i) != 0 {
                continue;
            }
            if events
                .iter()
                .enumerate()
                .any(|(j, prior)| used & (1 << j) == 0 && prior.end < event.start)
            {
                continue;
            }
            let (next, reply) = transition(state, event.action);
            if reply == event.reply && search(events, used | (1 << i), next) {
                return true;
            }
        }
        false
    }
    assert!(events.len() < 64);
    search(events, 0, None)
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
