//! 预分配的字节区仅发放互不重叠的租约；不可变缓存记录持有租约至最后一个读者退出。
use crate::types::Error;
use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    ptr::NonNull,
    sync::{Arc, Mutex},
};
struct State {
    free: Vec<(usize, usize)>,
    active: usize,
}
pub(super) struct Arena {
    pointer: NonNull<u8>,
    layout: Layout,
    state: Mutex<State>,
}
// SAFETY: 字节区不直接暴露引用；控制锁发放不重叠的 Lease，每个租约只允许独占初始化。
unsafe impl Send for Arena {}
// SAFETY: 已初始化租约只提供不可变访问；最后一个租约退出前其范围不会再次分配。
unsafe impl Sync for Arena {}
impl Arena {
    pub fn new(bytes: usize) -> Result<Arc<Self>, Error> {
        if bytes == 0 {
            return Err(Error::CapacityExceeded);
        }
        let layout = Layout::from_size_align(bytes, 8).map_err(|_| Error::CapacityExceeded)?;
        // SAFETY: 非零 Layout 已验证；空指针按分配失败处理。
        let pointer = NonNull::new(unsafe { alloc_zeroed(layout) }).ok_or(Error::OutOfMemory)?;
        let arena = Arc::new(Self {
            pointer,
            layout,
            state: Mutex::new(State {
                free: Vec::new(),
                active: 0,
            }),
        });
        {
            let mut state = arena
                .state
                .lock()
                .map_err(|_| Error::InvalidState("缓存字节区锁中毒"))?;
            state
                .free
                .try_reserve_exact(1)
                .map_err(|_| Error::OutOfMemory)?;
            state.free.push((0, bytes));
        }
        Ok(arena)
    }
    pub fn capacity(&self) -> usize {
        self.layout.size()
    }
    pub fn allocate(self: &Arc<Self>, len: usize) -> Result<Option<Lease>, Error> {
        if len == 0 || len > self.layout.size() {
            return Ok(None);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("缓存字节区锁中毒"))?;
        let Some(index) = state.free.iter().position(|&(_, bytes)| bytes >= len) else {
            return Ok(None);
        };
        // 每个活跃租约最多增加一个自由区间，归还时绝不再为元数据分配内存。
        let required = state
            .free
            .len()
            .checked_add(state.active)
            .and_then(|n| n.checked_add(1))
            .ok_or(Error::CapacityExceeded)?;
        let additional = required - state.free.len();
        state
            .free
            .try_reserve(additional)
            .map_err(|_| Error::OutOfMemory)?;
        let (offset, available) = state.free[index];
        if available == len {
            state.free.remove(index);
        } else {
            state.free[index] = (offset + len, available - len);
        }
        state.active += 1;
        Ok(Some(Lease {
            arena: self.clone(),
            offset,
            len,
        }))
    }
}
impl Drop for Arena {
    fn drop(&mut self) {
        // SAFETY: 最后一个 Arc 才执行；所有租约及其字节引用已退出，Layout 与分配一致。
        unsafe { dealloc(self.pointer.as_ptr(), self.layout) };
    }
}
pub(super) struct Lease {
    arena: Arc<Arena>,
    offset: usize,
    len: usize,
}
impl Lease {
    pub fn initialize(&mut self, bytes: &[u8]) {
        assert_eq!(self.len, bytes.len());
        // SAFETY: 自由区间账本保证该范围独占且位于分配内；&mut self 排除同一租约的共享访问。
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.arena.pointer.as_ptr().add(self.offset),
                self.len,
            )
        };
    }
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: 租约持有字节区 Arc，范围边界已验证；发布后不会再次可变访问同一租约。
        unsafe {
            std::slice::from_raw_parts(self.arena.pointer.as_ptr().add(self.offset), self.len)
        }
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.arena.state.lock().unwrap_or_else(|e| e.into_inner());
        state.active -= 1;
        state.free.push((self.offset, self.len));
        state.free.sort_unstable_by_key(|&(offset, _)| offset);
        let mut next = 0;
        for index in 0..state.free.len() {
            let (offset, len) = state.free[index];
            if next != 0 && state.free[next - 1].0 + state.free[next - 1].1 == offset {
                state.free[next - 1].1 += len;
            } else {
                state.free[next] = (offset, len);
                next += 1;
            }
        }
        state.free.truncate(next);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn 租约跨线程且空洞合并复用不会覆盖尚存读者() {
        let arena = Arena::new(64).unwrap();
        let mut a = arena.allocate(16).unwrap().unwrap();
        a.initialize(&[1; 16]);
        let mut b = arena.allocate(32).unwrap().unwrap();
        b.initialize(&[2; 32]);
        let mut c = arena.allocate(16).unwrap().unwrap();
        c.initialize(&[3; 16]);
        assert!(arena.allocate(1).unwrap().is_none());
        drop(a);
        drop(c);
        assert!(arena.allocate(32).unwrap().is_none());
        std::thread::scope(|scope| {
            scope
                .spawn(|| assert_eq!(b.bytes(), [2; 32]))
                .join()
                .unwrap();
        });
        drop(b);
        let mut all = arena.allocate(64).unwrap().unwrap();
        all.initialize(&[9; 64]);
        drop(arena);
        assert_eq!(all.bytes(), [9; 64]);
    }
}
