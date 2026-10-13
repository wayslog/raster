//! User key, value and operation contexts (FASTER `Key`/`Value`/context contract).
//!
//! Values in the mutable region are shared between threads, so in-place
//! callbacks only receive `&V`; concurrency safety is the value type's job
//! (atomics or a user lock), exactly as with FASTER `PutAtomic`/`RmwAtomic`.

/// A fixed-size key stored inline in the log.
pub trait Key: Copy + Eq + Send + Sync + 'static {
    /// 64-bit hash; bucket bits come from the low bits, the tag from bits 48..62.
    fn hash(&self) -> u64;
}

/// A fixed-size value stored inline in the log.
pub trait Value: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> Value for T {}

pub trait ReadContext<K: Key, V: Value> {
    fn key(&self) -> &K;
    /// Read from the immutable region.
    fn get(&mut self, value: &V);
    /// Read from the mutable region, where concurrent updates are possible.
    fn get_atomic(&mut self, value: &V) {
        self.get(value)
    }
    /// Called on the session thread when a `Pending` operation finishes.
    fn on_complete(&mut self, _status: crate::core::Status) {}
}

pub trait UpsertContext<K: Key, V: Value> {
    fn key(&self) -> &K;
    /// Value for a newly appended record.
    fn put(&mut self) -> V;
    /// In-place update in the mutable region; `false` falls back to append.
    fn put_atomic(&mut self, value: &V) -> bool;
    /// Called on the session thread when a `Pending` operation finishes.
    fn on_complete(&mut self, _status: crate::core::Status) {}
}

pub trait RmwContext<K: Key, V: Value> {
    fn key(&self) -> &K;
    /// Value when the key does not exist.
    fn rmw_initial(&mut self) -> V;
    /// New value computed from an immutable old value.
    fn rmw_copy(&mut self, old: &V) -> V;
    /// In-place update in the mutable region; `false` falls back to copy.
    fn rmw_atomic(&mut self, value: &V) -> bool;
    /// Called on the session thread when a `Pending` operation finishes.
    fn on_complete(&mut self, _status: crate::core::Status) {}
}

pub trait DeleteContext<K: Key> {
    fn key(&self) -> &K;
    /// Called on the session thread when a `Pending` operation finishes.
    fn on_complete(&mut self, _status: crate::core::Status) {}
}

/// FNV-1a over the eight little-endian bytes, matching the C++ benchmark key.
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
