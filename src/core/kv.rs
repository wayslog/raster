//! Inline keys, values, and operation contexts (FASTER context contract).
//!
//! Log data is copied bytewise and never dropped. Keys and values must not
//! own heap resources or contain pointers that become invalid after recovery.
//! Mutable values provide their own synchronization. Their allocated `size()`
//! stays constant for the record's lifetime, even if their logical length
//! changes. Size methods read only the fixed header, including on disk reads.
//!
//! A variable key can use `#[repr(C)] struct BytesKey { len: u32 }` followed
//! by `len` inline bytes. Its `size()` returns `size_of::<Self>() + len`, and
//! equality/hash inspect those bytes. A shallow context owns the bytes and
//! overrides `key_size`, `write_key`, `key_hash`, and `key_equals`, so its
//! `key()` need only return a header. Accessing a trailing payload requires
//! unsafe pointer operations justified by the complete record allocation.
use std::marker::PhantomData;
use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU32, AtomicU64};

/// An inline key; its size and bytes are immutable once published.
///
/// # Safety
/// The fixed header must accept every bit pattern, contain no references or
/// owned resources, and support bytewise relocation without destruction.
/// `size` may inspect only the fixed header. For a complete key it reports the
/// exact initialized allocation, at least size_of::<Self>(), and never changes.
/// Hashing and equality must not modify a published key. Access to trailing
/// bytes must be restricted to complete keys supplied by the engine.
///
/// ```compile_fail
/// use raster::core::Key;
/// #[derive(PartialEq, Eq)]
/// struct InvalidKey(u64);
/// impl Key for InvalidKey {
///     fn size(&self) -> u32 { 4096 }
///     fn hash(&self) -> u64 { 0 }
/// }
/// ```
pub unsafe trait Key: Eq + Sized + Send + Sync + 'static {
    #[inline(always)]
    fn size(&self) -> u32 {
        size_of::<Self>() as u32
    }
    /// Low hash bits select the bucket; bits 48..62 select the tag.
    fn hash(&self) -> u64;
}

/// An inline value with a fixed header and an optional trailing payload.
/// Implementations must initialize all bytes they inspect, report the exact
/// allocation size, and support bytewise relocation without destruction.
///
/// # Safety
/// The fixed header must accept every bit pattern and contain no references or
/// owned resources. `size` reads only that header and reports the immutable
/// allocation size, at least size_of::<Self>(), for every initialized value.
/// `initialize_empty` must initialize a valid value of exactly the supplied
/// slot size, including all bytes later copied or read. All in-place mutation
/// must preserve the allocation size and the type's validity invariants.
///
/// ```compile_fail
/// use raster::core::{Value, ValueSlot};
/// struct InvalidValue(u64);
/// impl Value for InvalidValue {
///     fn size(&self) -> u32 { 4096 }
///     fn initialize_empty(_: ValueSlot<'_, Self>) {}
/// }
/// ```
pub unsafe trait Value: Sized + Send + Sync + 'static {
    #[inline(always)]
    fn size(&self) -> u32 {
        size_of::<Self>() as u32
    }
    /// Initializes a valid value of exactly `slot.len()` bytes for a tombstone
    /// or abandoned allocation. Only its size will be inspected by the engine.
    fn initialize_empty(slot: ValueSlot<'_, Self>);
}

/// Exclusive, unpublished value storage owned by a record allocation.
pub struct ValueSlot<'a, V> {
    ptr: *mut V,
    len: u32,
    _borrow: PhantomData<&'a mut V>,
}

impl<V> ValueSlot<'_, V> {
    /// # Safety
    /// `ptr` is aligned for V, writable for `len` bytes, and exclusively owned
    /// for the returned lifetime. `len` is at least the size of V.
    #[inline(always)]
    pub(crate) unsafe fn new(ptr: *mut V, len: u32) -> Self {
        debug_assert!(len as usize >= size_of::<V>());
        Self {
            ptr,
            len,
            _borrow: PhantomData,
        }
    }
    /// Writes a fixed-size value. Variable values initialize the whole slot
    /// through `as_mut_ptr`, including their allocation-size header.
    #[inline(always)]
    pub fn write(self, value: V) {
        assert!(self.len as usize >= size_of::<V>());
        // SAFETY: the slot exclusively owns aligned, sufficiently large memory.
        unsafe { self.ptr.write(value) };
    }
    #[inline(always)]
    pub fn len(&self) -> u32 {
        self.len
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut V {
        self.ptr
    }
}

/// Shared key operations, including FASTER's shallow-key/deep-write contract.
///
/// # Safety
/// Sizes, hashes, equality and key bytes remain consistent through an operation
/// and its pending clones. `write_key` initializes exactly `key_size` bytes with
/// a key whose `size` equals that allocation. The default copy requires `key`
/// to point to that complete initialized allocation; a shallow key must override
/// the copy, hash and equality methods. Every clone owns all referenced request
/// data until completion. Methods must return normally rather than unwind.
///
/// ```compile_fail
/// use raster::core::KeyContext;
/// struct InvalidContext(u64);
/// impl KeyContext<u64> for InvalidContext {
///     fn key(&self) -> &u64 { &self.0 }
///     fn key_size(&self) -> u32 { 4096 }
/// }
/// ```
pub unsafe trait KeyContext<K: Key> {
    fn key(&self) -> &K;
    #[inline(always)]
    fn key_hash(&self) -> u64 {
        self.key().hash()
    }
    #[inline(always)]
    fn key_size(&self) -> u32 {
        self.key().size()
    }
    /// # Safety
    /// `dst` is aligned for K and writable for `key_size()` bytes. The default
    /// implementation requires `key()` to reference that many initialized bytes.
    #[inline(always)]
    unsafe fn write_key(&self, dst: *mut K) {
        // SAFETY: source and destination contain key_size bytes by contract;
        // the unpublished destination does not overlap the context.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.key() as *const K as *const u8,
                dst.cast(),
                self.key_size() as usize,
            )
        };
    }
    #[inline(always)]
    fn key_equals(&self, stored: &K) -> bool {
        stored == self.key()
    }
}

pub trait ReadContext<K: Key, V: Value>: KeyContext<K> {
    fn get(&mut self, value: &V);
    fn get_atomic(&mut self, value: &V) {
        self.get(value)
    }
    fn on_complete(&mut self, _status: crate::core::Status) {}
}

/// # Safety
/// `put` initializes every byte of a valid V allocation of exactly `value_size`
/// bytes. `put_atomic` preserves that allocation size and synchronizes shared
/// writes. Callbacks return normally, and pending clones uphold the same contract.
///
/// ```compile_fail
/// use raster::core::{KeyContext, UpsertContext, ValueSlot};
/// #[derive(Clone)]
/// struct MissingInitialization(u64);
/// unsafe impl KeyContext<u64> for MissingInitialization {
///     fn key(&self) -> &u64 { &self.0 }
/// }
/// impl UpsertContext<u64, u64> for MissingInitialization {
///     fn put(&mut self, _: ValueSlot<'_, u64>) {}
///     fn put_atomic(&mut self, _: &u64) -> bool { false }
/// }
/// ```
pub unsafe trait UpsertContext<K: Key, V: Value>: KeyContext<K> {
    #[inline(always)]
    fn value_size(&self) -> u32 {
        size_of::<V>() as u32
    }
    fn put(&mut self, slot: ValueSlot<'_, V>);
    /// Returns false if the existing allocation cannot hold the new value.
    fn put_atomic(&mut self, value: &V) -> bool;
    fn on_complete(&mut self, _status: crate::core::Status) {}
}

/// # Safety
/// Initial and copy callbacks initialize every byte of a valid value whose
/// allocation size equals the corresponding size callback. In-place mutation
/// preserves allocation size and synchronizes shared writes; a failed growth
/// attempt must prevent later in-place updates from racing the copy. Callbacks
/// return normally, and pending clones uphold the same contract.
///
/// ```compile_fail
/// use raster::core::{KeyContext, RmwContext, ValueSlot};
/// #[derive(Clone)]
/// struct MissingInitialization(u64);
/// unsafe impl KeyContext<u64> for MissingInitialization {
///     fn key(&self) -> &u64 { &self.0 }
/// }
/// impl RmwContext<u64, u64> for MissingInitialization {
///     fn rmw_initial(&mut self, _: ValueSlot<'_, u64>) {}
///     fn rmw_copy(&mut self, _: &u64, _: ValueSlot<'_, u64>) {}
///     fn rmw_atomic(&mut self, _: &u64) -> bool { false }
/// }
/// ```
pub unsafe trait RmwContext<K: Key, V: Value>: KeyContext<K> {
    #[inline(always)]
    fn initial_value_size(&self) -> u32 {
        size_of::<V>() as u32
    }
    #[inline(always)]
    fn copy_value_size(&self, _old: &V) -> u32 {
        size_of::<V>() as u32
    }
    fn rmw_initial(&mut self, slot: ValueSlot<'_, V>);
    fn rmw_copy(&mut self, old: &V, slot: ValueSlot<'_, V>);
    fn rmw_atomic(&mut self, value: &V) -> bool;
    fn on_complete(&mut self, _status: crate::core::Status) {}
}

/// # Safety
/// `value_size` must select an allocation the target store's Value implementation
/// can initialize to exactly that size. None selects the fixed header size.
/// The size remains stable throughout the operation and its pending clones.
///
/// ```compile_fail
/// use raster::core::{DeleteContext, KeyContext};
/// struct InvalidDelete(u64);
/// unsafe impl KeyContext<u64> for InvalidDelete {
///     fn key(&self) -> &u64 { &self.0 }
/// }
/// impl DeleteContext<u64> for InvalidDelete {
///     fn value_size(&self) -> Option<u32> { Some(4096) }
/// }
/// ```
pub unsafe trait DeleteContext<K: Key>: KeyContext<K> {
    /// None uses the fixed value-header size; variable tombstones may reserve
    /// additional payload. The value type initializes this allocation.
    #[inline(always)]
    fn value_size(&self) -> Option<u32> {
        None
    }
    fn on_complete(&mut self, _status: crate::core::Status) {}
}

macro_rules! values {
    ($($ty:ty),*) => { $(
    // SAFETY: scalar and atomic integer/float headers accept every bit pattern,
    // own no resources, and retain their fixed allocation size.
    unsafe impl Value for $ty {
        #[inline(always)]
        fn initialize_empty(slot: ValueSlot<'_, Self>) { slot.write(Self::default()); }
    })* };
}
values!(
    u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64, AtomicU32, AtomicU64, AtomicI32,
    AtomicI64
);

/// FNV-1a over little-endian bytes, matching the C++ benchmark key.
// SAFETY: u64 accepts every bit pattern and has a fixed, relocatable layout.
unsafe impl Key for u64 {
    #[inline(always)]
    fn hash(&self) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in self.to_le_bytes() {
            hash = (hash ^ byte as u64).wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }
}
// SAFETY: u32 accepts every bit pattern and has a fixed, relocatable layout.
unsafe impl Key for u32 {
    #[inline(always)]
    fn hash(&self) -> u64 {
        (*self as u64).hash()
    }
}
