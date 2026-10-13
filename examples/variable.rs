//! Minimal immutable inline byte values: cargo run --example variable.
//! Updates append a new record; no mutable payload synchronization is needed.
use raster::core::*;

#[repr(C)]
struct Bytes {
    len: u32,
}
impl Bytes {
    fn bytes(&self) -> &[u8] {
        // SAFETY: each stored Bytes has len initialized trailing bytes.
        unsafe {
            std::slice::from_raw_parts((self as *const Self).add(1).cast(), self.len as usize)
        }
    }
    fn initialize(mut slot: ValueSlot<'_, Self>, bytes: &[u8]) {
        let ptr = slot.as_mut_ptr();
        assert_eq!(slot.len() as usize, size_of::<Self>() + bytes.len());
        // SAFETY: the exclusive slot owns the header and the complete payload.
        unsafe {
            ptr.write(Self {
                len: bytes.len() as u32,
            });
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.add(1).cast(), bytes.len());
        }
    }
}
impl Value for Bytes {
    fn size(&self) -> u32 {
        (size_of::<Self>() as u32).saturating_add(self.len)
    }
    fn initialize_empty(mut slot: ValueSlot<'_, Self>) {
        let len = slot.len() - size_of::<Self>() as u32;
        let ptr = slot.as_mut_ptr();
        // SAFETY: the slot exclusively owns the header and len trailing bytes.
        unsafe {
            ptr.write(Self { len });
            std::ptr::write_bytes(ptr.add(1).cast::<u8>(), 0, len as usize);
        }
    }
}
#[derive(Clone)]
struct Request {
    key: u64,
    bytes: Vec<u8>,
}
impl KeyContext<u64> for Request {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl UpsertContext<u64, Bytes> for Request {
    fn value_size(&self) -> u32 {
        size_of::<Bytes>() as u32 + self.bytes.len() as u32
    }
    fn put(&mut self, slot: ValueSlot<'_, Bytes>) {
        Bytes::initialize(slot, &self.bytes);
    }
    fn put_atomic(&mut self, _: &Bytes) -> bool {
        false
    }
}
impl ReadContext<u64, Bytes> for Request {
    fn get(&mut self, value: &Bytes) {
        self.bytes = value.bytes().to_vec();
    }
}
fn main() {
    let store = FasterKv::<u64, Bytes>::new(StoreConfig {
        index_buckets: 64,
        log: raster::core::hlog::LogConfig {
            page_bits: 12,
            memory_pages: 8,
            mutable_fraction: 0.5,
        },
        ..Default::default()
    });
    let mut session = store.start_session();
    let mut request = Request {
        key: 7,
        bytes: b"hello".to_vec(),
    };
    assert_eq!(session.upsert(&mut request, 1), Status::Ok);
    request.bytes = b"hello, variable records".to_vec();
    assert_eq!(session.upsert(&mut request, 2), Status::Ok);
    request.bytes.clear();
    assert_eq!(session.read(&mut request, 3), Status::Ok);
    assert_eq!(request.bytes, b"hello, variable records");
    session.refresh();
    println!("value={}", String::from_utf8(request.bytes).unwrap());
}
