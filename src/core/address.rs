//! Logical hybrid-log addresses (FASTER `address.h`).
use std::sync::atomic::{AtomicU64, Ordering};

/// 48-bit logical address: `page:23 | offset:25`. The top bit of the page
/// field marks a read-cache address (FASTER `kReadCacheMask`), so hybrid-log
/// pages stay below 2^22.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Address(u64);

impl Address {
    pub const OFFSET_BITS: u32 = 25;
    pub const PAGE_BITS: u32 = 23;
    pub const ADDRESS_BITS: u32 = Self::OFFSET_BITS + Self::PAGE_BITS;
    pub const MAX_OFFSET: u64 = (1 << Self::OFFSET_BITS) - 1;
    pub const MAX_PAGE: u64 = (1 << (Self::PAGE_BITS - 1)) - 1;
    /// Largest hybrid-log address.
    pub const MAX: Address = Address(Self::READ_CACHE - 1);
    /// Set on index entries and addresses that point into the read cache.
    pub const READ_CACHE: u64 = 1 << (Self::ADDRESS_BITS - 1);
    /// Marks an index slot that is in use but has no record yet.
    pub const INVALID: Address = Address(1);

    #[inline(always)]
    pub const fn new(page: u64, offset: u64) -> Self {
        debug_assert!(page <= Self::MAX_PAGE && offset <= Self::MAX_OFFSET);
        Address((page << Self::OFFSET_BITS) | offset)
    }
    #[inline(always)]
    pub const fn from_control(control: u64) -> Self {
        Address(control & ((1 << Self::ADDRESS_BITS) - 1))
    }
    #[inline(always)]
    pub const fn control(self) -> u64 {
        self.0
    }
    #[inline(always)]
    pub const fn page(self) -> u64 {
        self.0 >> Self::OFFSET_BITS
    }
    #[inline(always)]
    pub const fn offset(self) -> u64 {
        self.0 & Self::MAX_OFFSET
    }
    #[inline(always)]
    pub const fn in_read_cache(self) -> bool {
        self.0 & Self::READ_CACHE != 0
    }
    /// The read-cache log address this entry address refers to.
    #[inline(always)]
    pub const fn read_cache_address(self) -> Self {
        Address(self.0 & !Self::READ_CACHE)
    }
    /// Tags a read-cache log address for use in index entries.
    #[inline(always)]
    pub const fn to_read_cache(self) -> Self {
        Address(self.0 | Self::READ_CACHE)
    }
    #[inline(always)]
    pub const fn add(self, delta: u64) -> Self {
        Address(self.0 + delta)
    }
}

#[derive(Debug, Default)]
#[repr(transparent)]
pub struct AtomicAddress(AtomicU64);

impl AtomicAddress {
    pub const fn new(address: Address) -> Self {
        Self(AtomicU64::new(address.0))
    }
    #[inline(always)]
    pub fn load(&self) -> Address {
        Address(self.0.load(Ordering::Acquire))
    }
    #[inline(always)]
    pub fn store(&self, address: Address) {
        self.0.store(address.0, Ordering::Release)
    }
    /// Advances to `address` if it is larger; returns true on change.
    pub fn monotonic_update(&self, address: Address) -> bool {
        self.fetch_max(address) < address
    }
    /// Advances to `address` if it is larger; returns the previous value.
    pub fn fetch_max(&self, address: Address) -> Address {
        Address(self.0.fetch_max(address.0, Ordering::AcqRel))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_and_offset_round_trip() {
        let a = Address::new(5, 1234);
        assert_eq!((a.page(), a.offset()), (5, 1234));
        assert_eq!(Address::from_control(a.control() | (1 << 60)), a);
        let cached = a.to_read_cache();
        assert!(cached.in_read_cache() && !a.in_read_cache());
        assert_eq!(cached.read_cache_address(), a);
        assert!(!Address::new(Address::MAX_PAGE, Address::MAX_OFFSET).in_read_cache());
    }

    #[test]
    fn monotonic_update_only_advances() {
        let a = AtomicAddress::new(Address::new(1, 0));
        assert!(a.monotonic_update(Address::new(2, 0)));
        assert!(!a.monotonic_update(Address::new(1, 5)));
        assert_eq!(a.load(), Address::new(2, 0));
    }
}
