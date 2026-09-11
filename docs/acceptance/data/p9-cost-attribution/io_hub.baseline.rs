//! 设备完成只跨线程传递拥有型缓冲；邮箱不持有用户请求或回调。
use crate::{
    device::{CompletionRoute, Device, IoCompletion},
    types::*,
};
use std::{collections::BTreeMap, sync::Mutex};
struct Mailbox {
    id: RequestId,
    completion: Option<IoCompletion>,
    completions: u64,
}
struct State {
    next: u64,
    mailboxes: BTreeMap<u64, Mailbox>,
}
pub(crate) struct CompletionHub {
    store: StoreId,
    capacity: usize,
    state: Mutex<State>,
    polling: Mutex<()>,
}
impl CompletionHub {
    pub fn new(store: StoreId, capacity: usize) -> Result<Self, Error> {
        store.validate()?;
        if capacity == 0 {
            return Err(Error::CapacityExceeded);
        }
        Ok(Self {
            store,
            capacity,
            state: Mutex::new(State {
                next: 0,
                mailboxes: BTreeMap::new(),
            }),
            polling: Mutex::new(()),
        })
    }
    pub fn reserve(&self, session: SessionId) -> Result<RequestId, Error> {
        session.validate()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("完成邮箱锁中毒"))?;
        if state.mailboxes.len() >= self.capacity {
            return Err(Error::Busy);
        }
        let next = state.next.checked_add(1).ok_or(Error::CapacityExceeded)?;
        let id = RequestId {
            store: self.store,
            session,
            slot: state.next,
            generation: Generation(0),
        };
        state.mailboxes.insert(
            id.slot,
            Mailbox {
                id,
                completion: None,
                completions: 0,
            },
        );
        state.next = next;
        Ok(id)
    }
    /// 路由编号在该设备所属引擎内单调分配，不复用槽号，耗尽则拒绝。
    pub fn route(id: RequestId) -> CompletionRoute {
        CompletionRoute(id.slot)
    }
    pub fn take(&self, id: RequestId) -> Result<Option<IoCompletion>, Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("完成邮箱锁中毒"))?;
        let mailbox = state
            .mailboxes
            .get_mut(&id.slot)
            .ok_or(Error::InvalidState("请求邮箱不存在"))?;
        if mailbox.id != id {
            return Err(Error::InvalidState("请求邮箱归属不匹配"));
        }
        Ok(mailbox.completion.take())
    }
    pub fn completion_count(&self, id: RequestId) -> Result<u64, Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("完成邮箱锁中毒"))?;
        state
            .mailboxes
            .get(&id.slot)
            .filter(|mailbox| mailbox.id == id)
            .map(|mailbox| mailbox.completions)
            .ok_or(Error::InvalidState("完成计数路由不存在"))
    }
    /// 请求终结或会话放弃时注销；设备仍拥有尚未返回的 I/O 缓冲。
    pub fn release(&self, id: RequestId) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("完成邮箱锁中毒"))?;
        if state
            .mailboxes
            .get(&id.slot)
            .is_none_or(|mailbox| mailbox.id != id)
        {
            return Err(Error::InvalidState("请求邮箱归属不匹配"));
        }
        state.mailboxes.remove(&id.slot);
        Ok(())
    }
    /// 仅在设备已 shutdown、所有发布线程退出后调用；终态只丢弃拥有型完成，不再投递任务。
    pub(crate) fn discard_after_device_shutdown(&self, device: &dyn Device) -> Result<(), Error> {
        // 设备 poll 的 panic 可能毒化序列化锁；关闭已终结设备，终态回收不恢复运行权限。
        let _polling = self
            .polling
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut completions = Vec::new();
        loop {
            device.poll(PollBudget::default(), &mut completions)?;
            if completions.is_empty() {
                break;
            }
            completions.clear();
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("完成邮箱锁中毒"))?;
        for mailbox in state.mailboxes.values_mut() {
            mailbox.completion = None;
        }
        Ok(())
    }
    pub fn poll(&self, device: &dyn Device, budget: PollBudget) -> Result<usize, Error> {
        let _polling = match self.polling.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(0),
            Err(_) => return Err(Error::InvalidState("完成轮询锁中毒")),
        };
        let mut completions = Vec::new();
        let limit = budget.0.get().min(self.capacity);
        let mut error = device
            .poll(
                PollBudget(std::num::NonZeroUsize::new(limit).expect("容量非零")),
                &mut completions,
            )
            .err();
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("完成邮箱锁中毒"))?;
        let count = completions.len();
        for completion in completions {
            if let Some(mailbox) = state.mailboxes.get_mut(&completion.route.0) {
                mailbox.completions = mailbox.completions.saturating_add(1);
                if mailbox.completion.is_some() {
                    error.get_or_insert(Error::InvalidState("一个请求存在多个未收取 I/O 完成"));
                } else {
                    mailbox.completion = Some(completion);
                }
            } else if completion.route.0 >= state.next {
                error.get_or_insert(Error::InvalidState("设备返回未分配的完成路由"));
            }
            // 已注销的历史路由仅释放完成缓冲，不调用已放弃的用户请求。
        }
        match error {
            Some(error) => Err(error),
            None => Ok(count),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::{IoOperation, IoRequest, memory::MemoryDevice};
    #[test]
    fn 其他会话轮询后结果留在原邮箱且错身份不能收取() {
        let hub = CompletionHub::new(StoreId([1; 16]), 2).unwrap();
        let device = MemoryDevice::new(4, 64).unwrap();
        let first = hub.reserve(SessionId([1; 16])).unwrap();
        let second = hub.reserve(SessionId([2; 16])).unwrap();
        assert!(matches!(hub.reserve(SessionId([3; 16])), Err(Error::Busy)));
        for id in [first, second] {
            device
                .submit(IoRequest {
                    route: CompletionHub::route(id),
                    operation: IoOperation::CreateDirectory(format!("目录{}", id.slot).into()),
                })
                .unwrap();
        }
        assert_eq!(hub.poll(&device, PollBudget::default()).unwrap(), 2);
        let mut wrong = first;
        wrong.session = second.session;
        assert!(hub.take(wrong).is_err());
        assert!(hub.release(wrong).is_err());
        assert!(hub.take(second).unwrap().unwrap().result.is_ok());
        assert!(hub.take(first).unwrap().unwrap().result.is_ok());
        assert!(hub.take(first).unwrap().is_none());
        hub.release(first).unwrap();
        let next = hub.reserve(first.session).unwrap();
        assert!(next.slot > second.slot);
    }
    #[test]
    fn 注销后迟到完成只回收缓冲且不会误投新请求() {
        let hub = CompletionHub::new(StoreId([1; 16]), 1).unwrap();
        let device = MemoryDevice::new(4, 64).unwrap();
        let old = hub.reserve(SessionId([1; 16])).unwrap();
        device
            .submit(IoRequest {
                route: CompletionHub::route(old),
                operation: IoOperation::Read {
                    file: crate::device::FileId {
                        slot: 999,
                        generation: Generation(0),
                    },
                    offset: 0,
                    buffer: crate::device::AlignedBuffer::new_zeroed(8, 8).unwrap(),
                },
            })
            .unwrap();
        hub.release(old).unwrap();
        let new = hub.reserve(old.session).unwrap();
        hub.poll(&device, PollBudget::default()).unwrap();
        assert!(hub.take(new).unwrap().is_none());
    }
    #[test]
    fn 重复未收完成与未知路由报错但不覆盖或丢失其他邮箱() {
        let hub = CompletionHub::new(StoreId([1; 16]), 4).unwrap();
        let device = MemoryDevice::new(4, 64).unwrap();
        let first = hub.reserve(SessionId([1; 16])).unwrap();
        let second = hub.reserve(SessionId([2; 16])).unwrap();
        let submit = |route, name: &str| {
            device
                .submit(IoRequest {
                    route,
                    operation: IoOperation::CreateDirectory(name.into()),
                })
                .unwrap()
        };
        let first_io = submit(CompletionHub::route(first), "第一次");
        submit(CompletionHub::route(first), "重复");
        let second_io = submit(CompletionHub::route(second), "另一个会话");
        assert!(matches!(
            hub.poll(&device, PollBudget::default()),
            Err(Error::InvalidState(_))
        ));
        assert_eq!(hub.take(first).unwrap().unwrap().id, first_io);
        assert!(hub.take(first).unwrap().is_none());
        assert_eq!(hub.take(second).unwrap().unwrap().id, second_io);
        submit(CompletionRoute(999), "未知路由");
        assert!(matches!(
            hub.poll(&device, PollBudget::default()),
            Err(Error::InvalidState(_))
        ));
        assert!(hub.take(first).unwrap().is_none());
        assert!(hub.take(second).unwrap().is_none());
    }
}
