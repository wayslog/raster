use super::*;
use crate::schema::{
    builtin::{ByteValueCodec, SerializedValue},
    value::ValueCodec,
};
use std::borrow::Cow;

struct LegacyCodec;
impl ValueCodec for LegacyCodec {
    type Value = Vec<u8>;
    fn format_id(&self) -> FormatId {
        ByteValueCodec.format_id()
    }
    fn encode(&self, value: &Vec<u8>) -> Result<Vec<u8>, Error> {
        if value.first() == Some(&0xff) {
            return Err(Error::Codec("Rejected value"));
        }
        ByteValueCodec.encode(value)
    }
    fn decode(&self, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        ByteValueCodec.decode(bytes)
    }
}

#[test]
fn default_encoding_view_preserves_legacy_encoding_and_errors() {
    let input = vec![0, 7, 42];
    let view = LegacyCodec.encode_view(&input).unwrap();
    assert!(matches!(view, Cow::Owned(_)));
    assert_eq!(&*view, input);
    assert!(matches!(
        LegacyCodec.encode_view(&vec![0xff]),
        Err(Error::Codec("Rejected value"))
    ));
    let pool = PagePool::new(4096, 1).unwrap();
    let layout = Arc::new(SerializedValue::new(LegacyCodec));
    let value = PageValue::initialize(&pool, layout, input.clone()).unwrap();
    assert!(matches!(
        value.update(|mut slot| slot.replace(&vec![0xff])),
        Err(Error::Codec("Rejected value"))
    ));
    assert_eq!(value.read(|v| v).unwrap(), input);
}

#[test]
fn borrowed_replacement_checks_capacity_before_writing_and_keeps_stable_bytes() {
    let pool = PagePool::new(4096, 1).unwrap();
    let layout = Arc::new(SerializedValue::new(ByteValueCodec));
    let value = PageValue::initialize(&pool, layout.clone(), vec![7, 8]).unwrap();
    assert!(
        value
            .update(|mut slot| slot.replace(&vec![1, 2, 3]))
            .is_err()
    );
    assert_eq!(value.read(|v| v).unwrap(), vec![7, 8]);
    value.update(|mut slot| slot.replace(&vec![0xff])).unwrap();
    let mut stable = [0];
    value.encode(&mut stable).unwrap();
    assert_eq!(stable, [0xff]);
    let restored = PageValue::decode(
        &pool,
        layout.clone(),
        &stable,
        layout.plan_decode(&stable).unwrap(),
    )
    .unwrap();
    assert_eq!(restored.read(|v| v).unwrap(), vec![0xff]);
    value.update(|mut slot| slot.replace(&vec![])).unwrap();
    value.encode(&mut []).unwrap();
    assert!(value.read(|v| v).unwrap().is_empty());
}
