//! Record header and inline layout (FASTER `record.h`).
use super::address::Address;
use super::kv::{Key, KeyContext, Value, ValueSlot};
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

/// Inline record layout; fixed-size trait methods constant-fold at call sites.
pub struct Layout<K, V>(PhantomData<(K, V)>);

const fn pad(size: usize, align: usize) -> usize {
    (size + align - 1) & !(align - 1)
}

impl<K, V> Layout<K, V> {
    pub const KEY_OFFSET: usize = pad(8, align_of::<K>());
    pub const ALIGN_OK: () = assert!(align_of::<K>() <= 8 && align_of::<V>() <= 8);

    #[inline(always)]
    pub const fn value_offset(key_size: u32) -> usize {
        pad(Self::KEY_OFFSET + key_size as usize, align_of::<V>())
    }
    #[inline(always)]
    pub const fn size(key_size: u32, value_size: u32) -> u32 {
        assert!(
            key_size as usize >= size_of::<K>(),
            "key header does not fit"
        );
        assert!(
            value_size as usize >= size_of::<V>(),
            "value header does not fit"
        );
        let size = pad(Self::value_offset(key_size) + value_size as usize, 8);
        assert!(size <= u32::MAX as usize, "record size exceeds u32");
        size as u32
    }
    #[inline(always)]
    pub const fn disk_size(key_size: u32, value_size: u32) -> u32 {
        let size = Self::value_offset(key_size) + value_size as usize;
        assert!(size <= u32::MAX as usize, "record size exceeds u32");
        size as u32
    }
    #[inline(always)]
    pub const fn min_disk_key_size() -> usize {
        Self::KEY_OFFSET + size_of::<K>()
    }
    #[inline(always)]
    pub const fn min_disk_value_size(key_size: u32) -> usize {
        Self::value_offset(key_size) + size_of::<V>()
    }
    #[inline(always)]
    pub const fn min_io_size() -> usize {
        Self::min_disk_value_size(size_of::<K>() as u32)
    }
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

impl<K: Key, V: Value> Record<K, V> {
    /// # Safety
    /// `ptr` must be 8-byte aligned, point to enough bytes for every accessed header or payload of a
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
    /// The key and value header must be initialized and the payload available.
    #[inline(always)]
    pub unsafe fn value(&self) -> &V {
        // SAFETY: initialized key and aligned value offset by contract.
        unsafe {
            &*self
                .ptr
                .add(Layout::<K, V>::value_offset(self.key().size()))
                .cast::<V>()
        }
    }
    /// # Safety
    /// The key and value headers must be initialized.
    #[inline(always)]
    pub unsafe fn size(&self) -> u32 {
        // SAFETY: both headers are initialized by contract.
        unsafe { Layout::<K, V>::size(self.key().size(), self.value().size()) }
    }
    /// # Safety
    /// The key and value headers must be initialized.
    #[inline(always)]
    pub unsafe fn disk_size(&self) -> u32 {
        // SAFETY: both headers are initialized by contract.
        unsafe { Layout::<K, V>::disk_size(self.key().size(), self.value().size()) }
    }
    /// Returns the next required prefix length, validating each available header.
    /// # Safety
    /// `available` initialized bytes exist at the aligned record pointer. Size
    /// methods must inspect only their fixed headers, as required by the traits.
    pub(crate) unsafe fn required_bytes(
        &self,
        available: usize,
        limit: usize,
    ) -> Result<usize, ()> {
        let key_header = Layout::<K, V>::min_disk_key_size();
        if key_header > limit {
            return Err(());
        }
        if available < key_header {
            return Ok(key_header);
        }
        // SAFETY: the key header is available; size reads only that header.
        let key_size = unsafe { self.key().size() };
        if (key_size as usize) < size_of::<K>() || key_size as usize > limit {
            return Err(());
        }
        let value_header = Layout::<K, V>::min_disk_value_size(key_size);
        if value_header > limit {
            return Err(());
        }
        if available < value_header {
            return Ok(value_header);
        }
        // SAFETY: the full key and value header are now available.
        let value_size = unsafe { self.value().size() };
        if (value_size as usize) < size_of::<V>() || value_size as usize > limit {
            return Err(());
        }
        let disk_size = Layout::<K, V>::value_offset(key_size) + value_size as usize;
        if disk_size > limit {
            return Err(());
        }
        Ok(disk_size)
    }
    /// Writes the key and returns exclusive storage for the value.
    /// # Safety
    /// This unpublished record owns Layout::size(ctx.key_size(), value_size)
    /// bytes. The context writes a complete key of the reported size.
    #[inline(always)]
    pub unsafe fn initialize_key<C: KeyContext<K>>(
        &self,
        ctx: &C,
        value_size: u32,
    ) -> ValueSlot<'_, V> {
        // SAFETY: aligned, exclusively owned allocation per contract.
        unsafe {
            ctx.write_key(self.ptr.add(Layout::<K, V>::KEY_OFFSET).cast());
            ValueSlot::new(
                self.ptr
                    .add(Layout::<K, V>::value_offset(ctx.key_size()))
                    .cast(),
                value_size,
            )
        }
    }
    /// # Safety
    /// The entire key and value must be initialized before publication.
    #[inline(always)]
    pub unsafe fn publish(&self, info: RecordInfo) {
        self.header_word().store(info.0, Ordering::Release);
    }
    /// Initializes from a complete, immutable record's raw words.
    /// # Safety
    /// The destination owns words.len() * 8 exclusive bytes; words contains
    /// a complete valid record, including trailing alignment padding.
    #[inline]
    pub unsafe fn initialize_from(&self, info: RecordInfo, words: &[u64]) {
        // SAFETY: exclusive destination and complete source by contract.
        unsafe {
            std::ptr::copy_nonoverlapping(
                words.as_ptr().cast::<u8>().add(8),
                self.ptr.add(8),
                words.len() * 8 - 8,
            );
            self.publish(info);
        }
    }
    /// # Safety
    /// The destination owns source.size() exclusive bytes; source is complete
    /// and immutable. Source padding need not be present in a disk buffer.
    #[inline]
    pub unsafe fn initialize_copy(&self, info: RecordInfo, source: &Record<K, V>) {
        // SAFETY: both payloads are in bounds by contract; no overlap.
        unsafe {
            std::ptr::copy_nonoverlapping(
                source.ptr.add(8),
                self.ptr.add(8),
                source.disk_size() as usize - 8,
            );
            self.publish(info);
        }
    }
    /// Initializes a tombstone or an allocation that will be marked invalid.
    /// # Safety
    /// Same allocation contract as initialize_key; unpublished destination.
    #[inline(always)]
    pub unsafe fn initialize_tombstone<C: KeyContext<K>>(
        &self,
        info: RecordInfo,
        ctx: &C,
        value_size: u32,
    ) {
        // SAFETY: exclusive allocation; initialize_empty writes a valid header.
        unsafe {
            V::initialize_empty(self.initialize_key(ctx, value_size));
            self.publish(info);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_record_matches_cpp_size() {
        assert_eq!(Layout::<u64, u64>::size(8, 8), 24);
        assert_eq!(Layout::<u32, u64>::value_offset(4), 16);
        assert_eq!(Layout::<u64, u64>::min_io_size(), 24);
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
        struct Context(u64);
        // SAFETY: The context owns a fixed-size integer key with stable bytes;
        // default hashing, equality and copying use that complete field.
        unsafe impl KeyContext<u64> for Context {
            fn key(&self) -> &u64 {
                &self.0
            }
        }
        let mut page = [0u64; 3];
        // SAFETY: local aligned buffer of the right size, single thread.
        let record = unsafe { Record::<u64, u64>::from_ptr(page.as_mut_ptr().cast()) };
        // SAFETY: freshly zeroed and unpublished.
        unsafe {
            record.initialize_key(&Context(7), 8).write(42);
            record.publish(RecordInfo::new(Address::INVALID, 0, false));
        }
        record.set_invalid();
        // SAFETY: initialized above.
        assert_eq!(unsafe { (*record.key(), *record.value()) }, (7, 42));
        assert!(record.info().invalid());
    }
}
