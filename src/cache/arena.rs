//! Pre-allocated byte areas are only issued non-overlapping leases;Immutable cache records hold a lease until the last reader exits.
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
// SAFETY: The byte area does not directly expose references;Control lock issuance without overlap Lease,Only exclusive initialization is allowed per lease.
unsafe impl Send for Arena {}
// SAFETY: Initialized leases only provide immutable access;The scope will not be allocated again until the last lease is exited..
unsafe impl Sync for Arena {}
impl Arena {
    pub fn new(bytes: usize) -> Result<Arc<Self>, Error> {
        if bytes == 0 {
            return Err(Error::CapacityExceeded);
        }
        let layout = Layout::from_size_align(bytes, 8).map_err(|_| Error::CapacityExceeded)?;
        // SAFETY: non-zero Layout Verified;Null pointers are handled as allocation failures.
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
                .map_err(|_| Error::InvalidState("Cache byte area lock poisoning"))?;
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
            .map_err(|_| Error::InvalidState("Cache byte area lock poisoning"))?;
        let Some(index) = state.free.iter().position(|&(_, bytes)| bytes >= len) else {
            return Ok(None);
        };
        // Each active lease adds at most one free interval,Memory is never allocated for metadata on return.
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
        // SAFETY: the last one Arc before execution;All leases and their byte references have been exited,Layout consistent with assignment.
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
        // SAFETY: The free range ledger guarantees that the range is exclusive and within the allocation;&mut self Exclude shared access for the same lease.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.arena.pointer.as_ptr().add(self.offset),
                self.len,
            )
        };
    }
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: The lease holds the byte area Arc,Range boundaries verified;The same lease is not mutably accessed again after publishing.
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
    fn the_lease_spans_threads_and_hole_merge_reuse_does_not_overwrite_existing_readers() {
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
