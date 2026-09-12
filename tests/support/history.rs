//! Legal order for independent enumeration of complete calling ranges;Do not read index,page or maintenance phase.
use raster::{
    api::operation::*,
    schema::{
        ValueRead, ValueUpdate,
        builtin::{AtomicU64Value, SchemaPair, U64Key},
    },
    types::Error,
};
use std::sync::atomic::Ordering;
pub type Schema = SchemaPair<U64Key, AtomicU64Value>;
#[derive(Clone, Copy, Debug)]
pub enum Action {
    Read,
    Put(u64, bool),
    Add(u64, bool),
    Delete,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reply {
    Value(u64),
    Deleted,
    Missing,
}
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub start: u64,
    pub end: u64,
    pub action: Action,
    pub reply: Reply,
}
pub struct Request(pub Action);
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
pub fn linearizable_from(events: &[Event], initial: Option<u64>) -> bool {
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
            let blind_delete = state.is_none()
                && matches!(event.action, Action::Delete)
                && event.reply == Reply::Deleted;
            if (reply == event.reply || blind_delete) && search(events, used | (1 << i), next) {
                return true;
            }
        }
        false
    }
    assert!(events.len() < 64);
    search(events, 0, initial)
}
