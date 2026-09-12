//! press Schema Decoding physical records;The value of an invalid slot does not have user value semantics,Cannot call value decoder.
use crate::{
    api::scan::ScannedRecord,
    format::Record,
    schema::{Schema, ValueLayout, key::decode_canonical},
    types::*,
};

pub(crate) fn decode_record<S: Schema>(
    schema: &S,
    address: LogAddress,
    bytes: &[u8],
) -> Result<ScannedRecord<S>, Error> {
    address.validate()?;
    let record = Record::decode(bytes)?;
    address.checked_add(bytes.len() as u64)?;
    if record
        .header
        .previous
        .is_some_and(|previous| previous >= address)
    {
        return Err(Error::InvalidFormat(
            "The scan record predecessor must be at a lower address",
        ));
    }
    let (key, _) = decode_canonical(schema.key_codec(), record.key)?;
    let value = if record.header.tombstone || record.header.invalid {
        None
    } else {
        Some(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                schema.value_layout().decode_owned(record.value)
            }))
            .map_err(|_| Error::InvalidState("Scan value decoding panic"))??,
        )
    };
    Ok(ScannedRecord {
        address,
        version: record.header.version,
        key,
        value,
        tombstone: record.header.tombstone,
        invalid: record.header.invalid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        format::RecordHeader,
        schema::{builtin::*, value::ValueCodec},
    };
    fn bytes(key: &[u8], value: &[u8], tombstone: bool, invalid: bool) -> Vec<u8> {
        let record = Record {
            header: RecordHeader {
                previous: None,
                version: CheckpointVersion(3),
                key_bytes: key.len() as u32,
                value_bytes: value.len() as u32,
                capacity_bytes: value.len() as u32,
                tombstone,
                invalid,
                final_record: false,
            },
            key,
            value,
        };
        let mut out = vec![0; record.header.encoded_len().unwrap()];
        record.encode(&mut out).unwrap();
        out
    }
    #[test]
    fn scan_decoding_has_null_key_variable_length_values_and_integer_boundaries() {
        let schema = SchemaPair::new(ByteKey, SerializedValue::new(ByteValueCodec));
        for key in [vec![], vec![0, 255]] {
            for value in [vec![], vec![0, 255, 128], vec![42; 1500]] {
                let input = bytes(&key, &value, false, false);
                let result = decode_record(&schema, LogAddress(0), &input).unwrap();
                drop(input);
                assert_eq!(result.key, key);
                assert_eq!(result.value, Some(value));
                assert_eq!(result.version, CheckpointVersion(3));
                assert!(!result.tombstone && !result.invalid);
            }
        }
        let schema = SchemaPair::new(U64Key, AtomicU64Value);
        for value in [0u64, 1, u64::MAX] {
            let input = bytes(&value.to_le_bytes(), &value.to_le_bytes(), false, false);
            let result = decode_record(&schema, LogAddress(0), &input).unwrap();
            assert_eq!(result.key, value);
            assert_eq!(result.value, Some(value));
        }
    }
    struct PanicCodec;
    impl ValueCodec for PanicCodec {
        type Value = Vec<u8>;
        fn format_id(&self) -> FormatId {
            ByteValueCodec.format_id()
        }
        fn encode(&self, _: &Vec<u8>) -> Result<Vec<u8>, Error> {
            panic!("should not be encoded")
        }
        fn decode(&self, _: &[u8]) -> Result<Vec<u8>, Error> {
            panic!("Inject decoding panic")
        }
    }
    #[test]
    fn tombstone_invalid_slot_preserves_keys_and_flags_and_does_not_call_the_value_decoder() {
        let schema = SchemaPair::new(ByteKey, SerializedValue::new(PanicCodec));
        for (value, tombstone, invalid) in [
            (&b""[..], true, false),
            (&b"bad"[..], false, true),
            (&b""[..], true, true),
        ] {
            let result = decode_record(
                &schema,
                LogAddress(0),
                &bytes(b"key", value, tombstone, invalid),
            )
            .unwrap();
            assert_eq!(result.key, b"key");
            assert!(result.value.is_none());
            assert_eq!((result.tombstone, result.invalid), (tombstone, invalid));
        }
        assert!(matches!(
            decode_record(&schema, LogAddress(0), &bytes(b"key", b"v", false, false)),
            Err(Error::InvalidState(_))
        ));
    }
    #[test]
    fn illegal_key_value_corruption_slots_and_address_overflows_are_rejected_instead_of_skipped() {
        let schema = SchemaPair::new(U64Key, AtomicU64Value);
        assert!(decode_record(&schema, LogAddress(0), &bytes(b"short", b"", false, true)).is_err());
        assert!(
            decode_record(
                &schema,
                LogAddress(0),
                &bytes(&0u64.to_le_bytes(), b"bad", false, false)
            )
            .is_err()
        );
        let mut input = bytes(&0u64.to_le_bytes(), &1u64.to_le_bytes(), false, false);
        assert!(decode_record(&schema, LogAddress::INVALID, &input).is_err());
        assert!(decode_record(&schema, LogAddress(u64::MAX - 2), &input).is_err());
        input[48] ^= 1;
        assert!(decode_record(&schema, LogAddress(0), &input).is_err());
    }
}
