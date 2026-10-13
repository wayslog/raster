//! Variable records with atomic payload bytes and a generation lock.
#![allow(dead_code)]
use raster::core::*;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

#[repr(C)]
#[derive(Clone)]
pub struct VarKey<const COLLIDE: bool = false> {
    pub len: u32,
}
impl<const C: bool> VarKey<C> {
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: stored keys have len initialized inline bytes after the header.
        unsafe {
            std::slice::from_raw_parts((self as *const Self).add(1).cast(), self.len as usize)
        }
    }
}
impl<const C: bool> PartialEq for VarKey<C> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes() == other.bytes()
    }
}
impl<const C: bool> Eq for VarKey<C> {}
impl<const C: bool> Key for VarKey<C> {
    fn size(&self) -> u32 {
        (size_of::<Self>() as u32).saturating_add(self.len)
    }
    fn hash(&self) -> u64 {
        if C { 0 } else { hash(self.bytes()) }
    }
}
pub fn hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ *b as u64).wrapping_mul(0x0100_0000_01b3)
    })
}

const LOCKED: u64 = 1 << 62;
const REPLACED: u64 = 1 << 63;
thread_local! {
    pub static SIZE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
#[repr(C)]
pub struct VarValue {
    generation: AtomicU64,
    capacity: u32,
    length: AtomicU32,
}
impl Value for VarValue {
    fn size(&self) -> u32 {
        SIZE_READS.with(|count| count.set(count.get() + 1));
        (size_of::<Self>() as u32).saturating_add(self.capacity)
    }
    fn initialize_empty(slot: ValueSlot<'_, Self>) {
        Self::initialize(slot, &[]);
    }
}
impl VarValue {
    pub fn size_for(len: usize) -> u32 {
        (size_of::<Self>() + len.next_multiple_of(8)) as u32
    }
    pub fn initialize(mut slot: ValueSlot<'_, Self>, bytes: &[u8]) {
        let capacity = slot.len() - size_of::<Self>() as u32;
        assert!(bytes.len() <= capacity as usize);
        let ptr = slot.as_mut_ptr();
        // SAFETY: slot exclusively owns header and capacity inline bytes.
        unsafe {
            ptr.write(Self {
                generation: AtomicU64::new(0),
                capacity,
                length: AtomicU32::new(bytes.len() as u32),
            });
            for i in 0..capacity as usize {
                ptr.add(1)
                    .cast::<AtomicU8>()
                    .add(i)
                    .write(AtomicU8::new(bytes.get(i).copied().unwrap_or(0)));
            }
        }
    }
    fn byte(&self, offset: usize) -> &AtomicU8 {
        assert!(offset < self.capacity as usize);
        // SAFETY: the initialized inline payload contains capacity AtomicU8s.
        unsafe { &*(self as *const Self).add(1).cast::<AtomicU8>().add(offset) }
    }
    pub fn snapshot(&self) -> Vec<u8> {
        loop {
            let before = self.generation.load(Ordering::SeqCst);
            if before & LOCKED != 0 {
                std::hint::spin_loop();
                continue;
            }
            let len = self.length.load(Ordering::SeqCst) as usize;
            let bytes = (0..len)
                .map(|i| self.byte(i).load(Ordering::SeqCst))
                .collect();
            if before == self.generation.load(Ordering::SeqCst) {
                return bytes;
            }
        }
    }
    fn lock(&self) -> bool {
        loop {
            let generation = self.generation.load(Ordering::SeqCst);
            if generation & REPLACED != 0 {
                return false;
            }
            if generation & LOCKED == 0
                && self
                    .generation
                    .compare_exchange(
                        generation,
                        generation | LOCKED,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
            {
                return true;
            }
            std::hint::spin_loop();
        }
    }
    fn unlock(&self, replaced: bool) {
        let current = self.generation.load(Ordering::SeqCst);
        self.generation.store(
            ((current & !LOCKED) + 1) | if replaced { REPLACED } else { 0 },
            Ordering::SeqCst,
        );
    }
    pub fn update(&self, update: impl FnOnce(Vec<u8>) -> Vec<u8>) -> bool {
        if !self.lock() {
            return false;
        }
        let old = (0..self.length.load(Ordering::SeqCst) as usize)
            .map(|i| self.byte(i).load(Ordering::SeqCst))
            .collect();
        let bytes = update(old);
        if bytes.len() > self.capacity as usize {
            self.unlock(true);
            return false;
        }
        for (i, byte) in bytes.iter().enumerate() {
            self.byte(i).store(*byte, Ordering::SeqCst);
        }
        self.length.store(bytes.len() as u32, Ordering::SeqCst);
        self.unlock(false);
        true
    }
}

pub type Output = Rc<RefCell<Option<(Status, Vec<u8>)>>>;
#[derive(Clone)]
pub struct VarRequest<const COLLIDE: bool = false> {
    pub key: VarKey<COLLIDE>,
    pub key_bytes: Vec<u8>,
    pub data: Vec<u8>,
    pub increment: u64,
    pub suffix: Vec<u8>,
    pub expected_rmw_status: Status,
    pub out: Output,
}
impl<const C: bool> VarRequest<C> {
    pub fn new(id: u64, key_len: usize, value_len: usize) -> Self {
        let mut key_bytes = vec![(id % 251) as u8; key_len.max(8)];
        key_bytes[..8].copy_from_slice(&id.to_le_bytes());
        Self {
            key: VarKey {
                len: key_bytes.len() as u32,
            },
            key_bytes,
            data: vec![88; value_len],
            increment: 0,
            suffix: Vec::new(),
            expected_rmw_status: Status::Ok,
            out: Rc::default(),
        }
    }
    fn updated(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        if self.increment != 0 {
            bytes.resize(bytes.len().max(8), 0);
            let value =
                u64::from_le_bytes(bytes[..8].try_into().unwrap()).wrapping_add(self.increment);
            bytes[..8].copy_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(&self.suffix);
        bytes
    }
    pub fn read(
        &mut self,
        session: &mut Session<'_, VarKey<C>, VarValue>,
        serial: u64,
    ) -> (Status, Vec<u8>, bool) {
        *self.out.borrow_mut() = None;
        let status = session.read(self, serial);
        let pending = status == Status::Pending;
        if pending {
            assert!(session.complete_pending(true));
        } else {
            <Self as ReadContext<VarKey<C>, VarValue>>::on_complete(self, status);
        }
        let (status, bytes) = self.out.borrow_mut().take().unwrap();
        (status, bytes, pending)
    }
}
impl<const C: bool> KeyContext<VarKey<C>> for VarRequest<C> {
    fn key(&self) -> &VarKey<C> {
        &self.key
    }
    fn key_hash(&self) -> u64 {
        if C { 0 } else { hash(&self.key_bytes) }
    }
    fn key_size(&self) -> u32 {
        size_of::<VarKey<C>>() as u32 + self.key_bytes.len() as u32
    }
    fn key_equals(&self, stored: &VarKey<C>) -> bool {
        stored.bytes() == self.key_bytes
    }
    unsafe fn write_key(&self, dst: *mut VarKey<C>) {
        // SAFETY: the destination owns key_size writable, aligned bytes.
        unsafe {
            dst.write(self.key.clone());
            std::ptr::copy_nonoverlapping(
                self.key_bytes.as_ptr(),
                dst.add(1).cast(),
                self.key_bytes.len(),
            );
        }
    }
}
impl<const C: bool> ReadContext<VarKey<C>, VarValue> for VarRequest<C> {
    fn get(&mut self, value: &VarValue) {
        self.data = value.snapshot();
    }
    fn on_complete(&mut self, status: Status) {
        *self.out.borrow_mut() = Some((status, self.data.clone()));
    }
}
impl<const C: bool> UpsertContext<VarKey<C>, VarValue> for VarRequest<C> {
    fn value_size(&self) -> u32 {
        VarValue::size_for(self.data.len())
    }
    fn put(&mut self, slot: ValueSlot<'_, VarValue>) {
        VarValue::initialize(slot, &self.data);
    }
    fn put_atomic(&mut self, value: &VarValue) -> bool {
        value.update(|_| self.data.clone())
    }
}
impl<const C: bool> RmwContext<VarKey<C>, VarValue> for VarRequest<C> {
    fn initial_value_size(&self) -> u32 {
        VarValue::size_for(self.updated(vec![]).len())
    }
    fn copy_value_size(&self, old: &VarValue) -> u32 {
        VarValue::size_for(old.capacity.max(8) as usize + self.suffix.len())
    }
    fn rmw_initial(&mut self, slot: ValueSlot<'_, VarValue>) {
        VarValue::initialize(slot, &self.updated(vec![]));
    }
    fn rmw_copy(&mut self, old: &VarValue, slot: ValueSlot<'_, VarValue>) {
        VarValue::initialize(slot, &self.updated(old.snapshot()));
    }
    fn rmw_atomic(&mut self, value: &VarValue) -> bool {
        value.update(|bytes| self.updated(bytes))
    }
    fn on_complete(&mut self, status: Status) {
        assert_eq!(status, self.expected_rmw_status);
        *self.out.borrow_mut() = Some((status, self.data.clone()));
    }
}
impl<const C: bool> DeleteContext<VarKey<C>> for VarRequest<C> {
    fn value_size(&self) -> Option<u32> {
        Some(VarValue::size_for(self.data.len()))
    }
}

pub type VarStore = FasterKv<VarKey, VarValue>;
pub struct TestDirectory(pub std::path::PathBuf);
impl TestDirectory {
    pub fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("raster-varlen-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub fn config(dir: &TestDirectory, resident: bool) -> StoreConfig {
    StoreConfig {
        index_buckets: 256,
        log_size_budget: 0,
        log: raster::core::hlog::LogConfig {
            page_bits: 12,
            memory_pages: if resident { 2048 } else { 8 },
            mutable_fraction: 0.5,
        },
        checkpoint_dir: Some(dir.0.join("checkpoints")),
    }
}
pub fn store(dir: &TestDirectory, resident: bool, cache: bool) -> VarStore {
    let device = std::sync::Arc::new(
        raster::core::device::FileDevice::open(dir.0.join("log"), 16, 2).unwrap(),
    );
    if cache {
        FasterKv::with_read_cache(
            config(dir, resident),
            device,
            ReadCacheConfig {
                log: raster::core::hlog::LogConfig {
                    page_bits: 12,
                    memory_pages: 4,
                    mutable_fraction: 0.5,
                },
            },
        )
    } else {
        FasterKv::with_device(config(dir, resident), device)
    }
}
pub fn session(store: &VarStore) -> Session<'_, VarKey, VarValue> {
    loop {
        if let Some(session) = store.try_start_session() {
            return session;
        }
        std::thread::yield_now();
    }
}
pub fn request(id: u64, len: usize) -> VarRequest {
    VarRequest::new(id, 8 + id as usize % 301, len)
}
pub fn finish_write(session: &mut Session<'_, VarKey, VarValue>, status: Status) {
    assert!(matches!(status, Status::Ok | Status::Pending), "{status:?}");
    if status == Status::Pending {
        assert!(session.complete_pending(true));
    }
}
