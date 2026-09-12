//! Only verify owned codes and slot plans;Page protection permission not constructed yet.
use raster::schema::{
    builtin::{AtomicU64Value, ByteValueCodec, SerializedValue, U64ValueCodec},
    value::{PreparedValue, ValueCodec, ValuePlan},
};

#[test]
fn ordinary_byte_values_overwrite_null_values_non_text_and_variable_length_slots() {
    let layout = SerializedValue::new(ByteValueCodec);
    for value in [vec![], vec![0, 255, 128], vec![42; 65537]] {
        let prepared = layout.prepare(&value).unwrap();
        assert_eq!(prepared.bytes(), value);
        assert_eq!(prepared.plan().live_bytes, value.len() + 8);
        assert_eq!(layout.decode_owned(prepared.bytes()).unwrap(), value);
        prepared.fits(value.len() + 8, 8).unwrap();
        if !value.is_empty() {
            assert!(prepared.fits(value.len() + 7, 8).is_err());
        }
    }
}
#[test]
fn ordinary_and_atomic_integer_fixed_little_endian_logic_encoding() {
    let normal = SerializedValue::new(U64ValueCodec);
    let atomic = AtomicU64Value;
    assert_ne!(normal.format_id(), atomic.format_id());
    assert_eq!(normal.format_id().0, *b"raster:valu64le1");
    assert_eq!(atomic.format_id().0, *b"raster:atomic641");
    for value in [0, 1, 1 << 63, u64::MAX, 0x0102030405060708] {
        let n = normal.prepare(&value).unwrap();
        let a = atomic.prepare(value).unwrap();
        assert_eq!(n.bytes(), value.to_le_bytes());
        assert_eq!(a.bytes(), n.bytes());
        assert_eq!(normal.decode_owned(n.bytes()).unwrap(), value);
        assert_eq!(atomic.decode_owned(a.bytes()).unwrap(), value);
        assert_eq!(
            a.plan().alignment,
            std::mem::align_of::<std::sync::atomic::AtomicU64>()
        );
        assert!(a.fits(7, 8).is_err());
        assert!(a.fits(8, 1).is_err());
    }
    assert_eq!(
        atomic.prepare(0x0102030405060708).unwrap().bytes(),
        &[8, 7, 6, 5, 4, 3, 2, 1]
    );
    for len in [0, 1, 7, 9, 16] {
        assert!(normal.decode_owned(&vec![0; len]).is_err());
        assert!(atomic.decode_owned(&vec![0; len]).is_err());
    }
}
#[test]
fn both_active_and_coded_sizes_are_limited_and_allocation_layout_does_not_overflow() {
    let p = PreparedValue::new(vec![0; 16], 8, 8).unwrap();
    assert_eq!(p.plan().capacity, 16);
    assert!(p.fits(15, 8).is_err());
    let p = PreparedValue::new(vec![0; 8], 16, 8).unwrap();
    assert!(p.fits(15, 8).is_err());
    for alignment in [0, 3, usize::MAX] {
        assert!(p.fits(16, alignment).is_err());
    }
    assert!(
        ValuePlan {
            live_bytes: 0,
            encoded_bytes: 0,
            capacity: usize::MAX,
            alignment: 8
        }
        .validate()
        .is_err()
    );
    assert!(
        ValuePlan {
            live_bytes: 0,
            encoded_bytes: 0,
            capacity: 1,
            alignment: 1usize << (usize::BITS - 1)
        }
        .validate()
        .is_err()
    );
}
#[test]
fn decoding_failure_releases_the_temporary_owned_value_and_does_not_generate_a_slot_license() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Temporary(Arc<AtomicUsize>);
    impl Drop for Temporary {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Failing(Arc<AtomicUsize>);
    impl ValueCodec for Failing {
        type Value = Temporary;
        fn format_id(&self) -> raster::types::FormatId {
            raster::types::FormatId([1; 16])
        }
        fn encode(&self, _: &Temporary) -> Result<Vec<u8>, raster::types::Error> {
            Err(raster::types::Error::Codec("Encoding failed"))
        }
        fn decode(&self, _: &[u8]) -> Result<Temporary, raster::types::Error> {
            let _temporary = Temporary(self.0.clone());
            Err(raster::types::Error::Codec("Decoding failed"))
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let layout = SerializedValue::new(Failing(drops.clone()));
    assert!(layout.decode_owned("damaged".as_bytes()).is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let value = Temporary(drops.clone());
    assert!(layout.prepare(&value).is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(value);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}
