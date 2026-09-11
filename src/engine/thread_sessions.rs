//! 每实例的线程会话名额；失败准备与成功关闭都通过拥有型租约归还，不使用全局 TLS。
use crate::types::Error;
use std::{
    sync::{Arc, Mutex},
    thread::ThreadId,
};
pub(crate) struct ThreadSessions {
    limit: usize,
    active: Mutex<Vec<ThreadId>>,
}
impl ThreadSessions {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            active: Mutex::new(Vec::new()),
        }
    }
    pub fn enter(self: &Arc<Self>) -> Result<ThreadSession, Error> {
        let thread = std::thread::current().id();
        let mut active = self
            .active
            .lock()
            .map_err(|_| Error::InvalidState("线程会话名额锁中毒"))?;
        if active.len() >= self.limit {
            return Err(Error::CapacityExceeded);
        }
        if active.contains(&thread) {
            return Err(Error::Busy);
        }
        active.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        active.push(thread);
        Ok(ThreadSession {
            registry: self.clone(),
            thread,
        })
    }
}
pub(crate) struct ThreadSession {
    registry: Arc<ThreadSessions>,
    thread: ThreadId,
}
impl Drop for ThreadSession {
    fn drop(&mut self) {
        // 名额锁内没有用户代码；异常收尾仍归还原线程身份，不依赖 Drop 的调用线程。
        let mut active = self
            .registry
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        active.retain(|thread| *thread != self.thread);
    }
}
