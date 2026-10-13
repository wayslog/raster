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
pub trait Key: Eq + Sized + Send + Sync + 'static {
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
pub trait Value: Sized + Send + Sync + 'static {
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
pub trait KeyContext<K: Key> {
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

pub trait UpsertContext<K: Key, V: Value>: KeyContext<K> {
    #[inline(always)]
    fn value_size(&self) -> u32 {
        size_of::<V>() as u32
    }
    fn put(&mut self, slot: ValueSlot<'_, V>);
    /// Returns false if the existing allocation cannot hold the new value.
    fn put_atomic(&mut self, value: &V) -> bool;
    fn on_complete(&mut self, _status: crate::core::Status) {}
}

pub trait RmwContext<K: Key, V: Value>: KeyContext<K> {
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

pub trait DeleteContext<K: Key>: KeyContext<K> {
    /// None uses the fixed value-header size; variable tombstones may reserve
    /// additional payload. The value type initializes this allocation.
    #[inline(always)]
    fn value_size(&self) -> Option<u32> {
        None
    }
    fn on_complete(&mut self, _status: crate::core::Status) {}
}

macro_rules! values {
    ($($ty:ty),*) => { $(impl Value for $ty {
        #[inline(always)]
        fn initialize_empty(slot: ValueSlot<'_, Self>) { slot.write(Self::default()); }
    })* };
}
values!(
    u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64, AtomicU32, AtomicU64, AtomicI32,
    AtomicI64
);

/// FNV-1a over little-endian bytes, matching the C++ benchmark key.
impl Key for u64 {
    #[inline(always)]
    fn hash(&self) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in self.to_le_bytes() {
            hash = (hash ^ byte as u64).wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }
}
impl Key for u32 {
    #[inline(always)]
    fn hash(&self) -> u64 {
        (*self as u64).hash()
    }
}
