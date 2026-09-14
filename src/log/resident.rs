//! Session-local hints borrow resident records only under the matching epoch.
use super::*;
use crate::epoch::EpochGuard;
use std::{
    ptr::NonNull,
    sync::{Weak, atomic::Ordering},
};
const SLOTS: usize = 512;
struct Hint<V: ValueLayout> {
    address: LogAddress,
    epoch: EpochVersion,
    value: NonNull<value::PageValue<V>>,
}
pub(crate) struct ResidentHints<V: ValueLayout> {
    owner: Weak<LogControl>,
    entries: Vec<Option<Hint<V>>>,
}
impl<V: ValueLayout> Default for ResidentHints<V> {
    fn default() -> Self {
        Self {
            owner: Weak::new(),
            entries: Vec::new(),
        }
    }
}
impl<V: ValueLayout> ResidentHints<V> {
    pub fn clear(&mut self) {
        self.entries = Vec::new();
        self.owner = Weak::new();
    }
    fn bind(&mut self, log: &HybridLog<V>) {
        if self.owner.as_ptr() != Arc::as_ptr(&log.state) {
            self.entries.clear();
            self.owner = Arc::downgrade(&log.state);
        }
        if self.entries.is_empty() && self.entries.try_reserve_exact(SLOTS).is_ok() {
            self.entries.resize_with(SLOTS, || None);
        }
    }
}
pub(crate) struct ReadHint<'a, 'epoch, V: ValueLayout> {
    pub guard: &'a EpochGuard<'epoch>,
    pub cache: &'a mut ResidentHints<V>,
}
pub(crate) struct BorrowedRecord<'a, V: ValueLayout> {
    value: &'a value::PageValue<V>,
    local: PhantomData<Rc<()>>,
}
impl<V: ValueLayout> BorrowedRecord<'_, V> {
    pub fn is_tombstone(&self) -> bool {
        self.value.is_tombstone()
    }
    pub fn try_read_live<R>(
        &self,
        read: impl for<'a> FnOnce(V::Read<'a>) -> R,
    ) -> Result<ValueAccess<Option<R>>, Error> {
        self.value.try_read_live(read)
    }
}
impl<V: ValueLayout> RecordLease<V> {
    pub fn borrow(&self) -> BorrowedRecord<'_, V> {
        BorrowedRecord {
            value: &self.value,
            local: PhantomData,
        }
    }
}
impl<V: ValueLayout> HybridLog<V> {
    pub fn resident_head_borrowed<'a>(
        &'a self,
        key: &[u8],
        head: Option<LogAddress>,
        hint: &'a mut ReadHint<'_, '_, V>,
    ) -> Result<Option<BorrowedRecord<'a, V>>, Error> {
        let Some(address) = head else { return Ok(None) };
        address.validate()?;
        if !self.resident_range_contains(address)? {
            return Ok(None);
        }
        let manager = self
            .state
            .epoch
            .get()
            .ok_or(Error::InvalidState("log epoch is not bound"))?;
        let epoch = hint.guard.protects(manager)?;
        if self.records.is_poisoned() {
            return Err(Error::InvalidState("Record table lock poisoning"));
        }
        hint.cache.bind(self);
        let slot = ((address.0 >> 3).wrapping_mul(0x9e3779b97f4a7c15) >> 55) as usize;
        let cached = hint
            .cache
            .entries
            .get(slot)
            .and_then(Option::as_ref)
            .filter(|entry| entry.address == address && entry.epoch == epoch)
            .map(|entry| entry.value);
        let pointer = if let Some(value) = cached {
            value
        } else {
            let records = self
                .records
                .lock()
                .map_err(|_| Error::InvalidState("Record table lock poisoning"))?;
            let Some(value) = records.get(&address) else {
                drop(records);
                return if address < self.frontiers()?.head {
                    Ok(None)
                } else {
                    Err(Error::RangeTruncated)
                };
            };
            let pointer = NonNull::from(&**value);
            if let Some(entry) = hint.cache.entries.get_mut(slot) {
                *entry = Some(Hint {
                    address,
                    epoch,
                    value: pointer,
                });
            }
            pointer
        };
        if self.records.is_poisoned() {
            return Err(Error::InvalidState("Record table lock poisoning"));
        }
        // SAFETY: The hint is bound to this log's still-allocated control block.
        // Its pointer was acquired under the directory lock and matching guard.
        // Every unlink retains an Arc before removal and advances that manager's
        // epoch. An equal guard epoch prevents collection until the guard exits;
        // stale epochs are rejected before dereferencing a cached pointer. The
        // returned borrow cannot outlive the log or the borrowed ReadHint/guard.
        let value = unsafe { pointer.as_ref() };
        if !value.visible.load(Ordering::SeqCst) {
            return if address < self.frontiers()?.head {
                Ok(None)
            } else {
                Err(Error::RangeTruncated)
            };
        }
        Ok((value.key() == key).then_some(BorrowedRecord {
            value,
            local: PhantomData,
        }))
    }
}
