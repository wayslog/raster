//! Record header and inline layout (FASTER `record.h`).
use super::address::Address;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

/// `previous:48 | version:13 | invalid:1 | tombstone:1 | final:1`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct RecordInfo(u64);

impl RecordInfo {
    const VERSION_SHIFT: u32 = 48;
    const VERSION_MASK: u64 = (1 << 13) - 1;
    const INVALID: u64 = 1 << 61;
    const TOMBSTONE: u64 = 1 << 62;
    const FINAL: u64 = 1 << 63;

    #[inline(always)]
    pub fn new(previous: Address, version: u32, tombstone: bool) -> Self {
        let mut word =
            previous.control() | ((version as u64 & Self::VERSION_MASK) << Self::VERSION_SHIFT);
        if tombstone {
            word |= Self::TOMBSTONE;
        }
        Self(word)
    }
    /// Reinterprets a raw header word read from the log.
    #[inline(always)]
    pub fn from_word(word: u64) -> Self {
        Self(word)
    }
    #[inline(always)]
    pub fn previous(self) -> Address {
        Address::from_control(self.0)
    }
    #[inline(always)]
    pub fn version(self) -> u32 {
        ((self.0 >> Self::VERSION_SHIFT) & Self::VERSION_MASK) as u32
    }
    #[inline(always)]
    pub fn invalid(self) -> bool {
        self.0 & Self::INVALID != 0
    }
    #[inline(always)]
    pub fn tombstone(self) -> bool {
        self.0 & Self::TOMBSTONE != 0
    }
    #[inline(always)]
    pub fn is_final(self) -> bool {
        self.0 & Self::FINAL != 0
    }
    /// An all-zero header marks unused space at the end of a page.
    #[inline(always)]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }
}

/// Compile-time record layout for fixed-size keys and values.
pub struct Layout<K, V>(PhantomData<(K, V)>);

const fn pad(size: usize, align: usize) -> usize {
    (size + align - 1) & !(align - 1)
}
const fn max(a: usize, b: usize) -> usize {
    if a > b { a } else { b }
}

impl<K, V> Layout<K, V> {
    pub const KEY_OFFSET: usize = pad(8, align_of::<K>());
    pub const VALUE_OFFSET: usize = pad(Self::KEY_OFFSET + size_of::<K>(), align_of::<V>());
    pub const SIZE: usize = pad(Self::VALUE_OFFSET + size_of::<V>(), 8);
    /// Records are placed at 8-byte aligned log offsets.
    pub const ALIGN_OK: () = assert!(max(align_of::<K>(), align_of::<V>()) <= 8);
}

/// Borrowed view of a record inside a log page.
pub struct Record<K, V> {
    ptr: *mut u8,
    _types: PhantomData<(K, V)>,
}

impl<K, V> Clone for Record<K, V> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<K, V> Copy for Record<K, V> {}

impl<K, V> Record<K, V> {
    /// # Safety
    /// `ptr` must be 8-byte aligned, point to `Layout::<K, V>::SIZE` bytes of a
    /// live log page, and stay valid for the caller's epoch protection.
    #[inline(always)]
    pub unsafe fn from_ptr(ptr: *mut u8) -> Self {
        let () = Layout::<K, V>::ALIGN_OK;
        Self {
            ptr,
            _types: PhantomData,
        }
    }
    #[inline(always)]
    fn header_word(&self) -> &AtomicU64 {
        // SAFETY: `from_ptr` guarantees an aligned, live 8-byte header.
        unsafe { &*(self.ptr as *const AtomicU64) }
    }
    #[inline(always)]
    pub fn info(&self) -> RecordInfo {
        RecordInfo(self.header_word().load(Ordering::Acquire))
    }
    #[inline(always)]
    pub fn set_invalid(&self) {
        self.header_word()
            .fetch_or(RecordInfo::INVALID, Ordering::AcqRel);
    }
    #[inline(always)]
    pub fn set_tombstone(&self) {
        self.header_word()
            .fetch_or(RecordInfo::TOMBSTONE, Ordering::AcqRel);
    }
    /// # Safety
    /// The key must have been initialized before the record was published.
    #[inline(always)]
    pub unsafe fn key(&self) -> &K {
        // SAFETY: layout offsets are aligned for `K`; initialized per contract.
        unsafe { &*(self.ptr.add(Layout::<K, V>::KEY_OFFSET) as *const K) }
    }
    /// # Safety
    /// The value must have been initialized before the record was published.
    #[inline(always)]
    pub unsafe fn value(&self) -> &V {
        // SAFETY: layout offsets are aligned for `V`; initialized per contract.
        unsafe { &*(self.ptr.add(Layout::<K, V>::VALUE_OFFSET) as *const V) }
    }
    /// Initializes an unpublished record.
    ///
    /// # Safety
    /// The record must be freshly allocated and not yet reachable by others.
    #[inline(always)]
    pub unsafe fn initialize(&self, info: RecordInfo, key: K, value: V) {
        // SAFETY: exclusive, aligned, unpublished memory per contract.
        unsafe {
            (self.ptr.add(Layout::<K, V>::KEY_OFFSET) as *mut K).write(key);
            (self.ptr.add(Layout::<K, V>::VALUE_OFFSET) as *mut V).write(value);
        }
        self.header_word().store(info.0, Ordering::Release);
    }
    /// Initializes an unpublished record whose value is never read.
    ///
    /// # Safety
    /// Same as [`Record::initialize`]; `info` must carry the tombstone or the
    /// record must be marked invalid before anyone can reach it.
    #[inline(always)]
    pub unsafe fn initialize_tombstone(&self, info: RecordInfo, key: K) {
        // SAFETY: exclusive, aligned, unpublished memory per contract.
        unsafe { (self.ptr.add(Layout::<K, V>::KEY_OFFSET) as *mut K).write(key) };
        self.header_word().store(info.0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_record_matches_cpp_size() {
        assert_eq!(Layout::<u64, u64>::SIZE, 24);
        assert_eq!(Layout::<u32, u64>::VALUE_OFFSET, 16);
    }

    #[test]
    fn header_bits_round_trip() {
        let info = RecordInfo::new(Address::new(3, 64), 5, true);
        assert_eq!(info.previous(), Address::new(3, 64));
        assert_eq!(info.version(), 5);
        assert!(info.tombstone() && !info.invalid() && !info.is_final());
    }

    #[test]
    fn initialize_and_mark() {
        let mut page = [0u64; 3];
        // SAFETY: local aligned buffer of the right size, single thread.
        let record = unsafe { Record::<u64, u64>::from_ptr(page.as_mut_ptr().cast()) };
        // SAFETY: freshly zeroed and unpublished.
        unsafe { record.initialize(RecordInfo::new(Address::INVALID, 0, false), 7, 42) };
        record.set_invalid();
        // SAFETY: initialized above.
        assert_eq!(unsafe { (*record.key(), *record.value()) }, (7, 42));
        assert!(record.info().invalid());
    }
}
