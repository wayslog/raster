//! Stable hashing versus explicit key encoding;Cannot use default Hash platform-dependent input freeze format.
use crate::types::{Error, FormatId, HashDescriptor, KeyHash};
use std::borrow::Borrow;

/// The encoding is the canonical byte representation;Equal keys must have the same encoding and hash.
/// Encoding failure must not partially modify the output,The output length must match exactly encoded_len.
/// encoded version,Algorithm or seed changes must be rejected before records can be read;Cannot silently rebuild indexes to mask changes.
pub trait KeyCodec: Send + Sync + 'static {
    type Key: ?Sized;
    /// Owning keys must be borrowable as the same logical key,For recovery and scanning to recalculate stable hashes.
    type OwnedKey: Borrow<Self::Key> + 'static;
    fn format_id(&self) -> FormatId;
    fn hash_descriptor(&self) -> HashDescriptor;
    fn hash(&self, key: &Self::Key) -> KeyHash;
    fn encoded_len(&self, key: &Self::Key) -> Result<u32, Error>;
    fn encode(&self, key: &Self::Key, output: &mut [u8]) -> Result<(), Error>;
    fn equals_encoded(&self, key: &Self::Key, encoded: &[u8]) -> Result<bool, Error>;
    fn decode_owned(&self, encoded: &[u8]) -> Result<Self::OwnedKey, Error>;

    /// Check the key semantics of persistent material declarations;Recovery portal verifies format before interpreting records,Algorithms and seeds.
    fn validate_identity(&self, format: FormatId, hash: &HashDescriptor) -> Result<(), Error> {
        if self.format_id() != format {
            return Err(Error::InvalidFormat("Key encoding version mismatch"));
        }
        if &self.hash_descriptor() != hash {
            return Err(Error::InvalidFormat("Key hash algorithm or seed mismatch"));
        }
        Ok(())
    }
}

/// Get canonical key and full hash from persistent bytes;Results can only be used for verified Schema identity material.
pub(crate) fn decode_canonical<K: KeyCodec>(
    codec: &K,
    encoded: &[u8],
) -> Result<(K::OwnedKey, KeyHash), Error> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let owned = codec.decode_owned(encoded)?;
        let key = owned.borrow();
        if codec.encoded_len(key)? as usize != encoded.len() {
            return Err(Error::InvalidFormat(
                "Decoding key length does not conform to specification encoding",
            ));
        }
        let mut canonical = Vec::new();
        canonical
            .try_reserve_exact(encoded.len())
            .map_err(|_| Error::OutOfMemory)?;
        canonical.resize(encoded.len(), 0);
        codec.encode(key, &mut canonical)?;
        if canonical != encoded || !codec.equals_encoded(key, encoded)? {
            return Err(Error::InvalidFormat(
                "Recovery key is not canonical encoding",
            ));
        }
        let hash = codec.hash(key);
        Ok((owned, hash))
    }))
    .map_err(|_| Error::InvalidState("Recovery key decoding or hash panic"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::builtin::{ByteKey, U64Key};
    #[test]
    fn recovery_key_canonical_round_trip_to_get_full_stable_hash() {
        for bytes in [vec![], vec![0, 255, 128], vec![42; 65537]] {
            let (owned, hash) = decode_canonical(&ByteKey, &bytes).unwrap();
            assert_eq!(owned, bytes);
            assert_eq!(hash, ByteKey.hash(&bytes));
        }
        for key in [0, 1, 8969, 9239, u64::MAX] {
            let (owned, hash) = decode_canonical(&U64Key, &key.to_le_bytes()).unwrap();
            assert_eq!(owned, key);
            assert_eq!(hash, U64Key.hash(&key));
        }
        assert_ne!(
            decode_canonical(&U64Key, &8969u64.to_le_bytes()).unwrap().1,
            decode_canonical(&U64Key, &9239u64.to_le_bytes()).unwrap().1
        );
        assert!(decode_canonical(&U64Key, &[0; 7]).is_err());
    }
    struct Owned(u64);
    impl Borrow<u64> for Owned {
        fn borrow(&self) -> &u64 {
            &self.0
        }
    }
    struct Codec {
        alias: bool,
        panic_hash: bool,
    }
    impl KeyCodec for Codec {
        type Key = u64;
        type OwnedKey = Owned;
        fn format_id(&self) -> FormatId {
            U64Key.format_id()
        }
        fn hash_descriptor(&self) -> HashDescriptor {
            U64Key.hash_descriptor()
        }
        fn hash(&self, key: &u64) -> KeyHash {
            assert!(!self.panic_hash, "Inject recovery hash panic");
            U64Key.hash(key)
        }
        fn encoded_len(&self, key: &u64) -> Result<u32, Error> {
            U64Key.encoded_len(key)
        }
        fn encode(&self, key: &u64, output: &mut [u8]) -> Result<(), Error> {
            U64Key.encode(key, output)
        }
        fn equals_encoded(&self, _: &u64, _: &[u8]) -> Result<bool, Error> {
            Ok(true)
        }
        fn decode_owned(&self, bytes: &[u8]) -> Result<Owned, Error> {
            let value = U64Key.decode_owned(bytes)?;
            Ok(Owned(if self.alias { value & !1 } else { value }))
        }
    }
    #[test]
    fn custom_owned_keys_are_borrowable_and_non_canonical_aliases_and_hash_panic_rejects() {
        let bytes = 3u64.to_le_bytes();
        let codec = Codec {
            alias: false,
            panic_hash: false,
        };
        let (owned, hash) = decode_canonical(&codec, &bytes).unwrap();
        assert_eq!(*Borrow::<u64>::borrow(&owned), 3);
        assert_eq!(hash, U64Key.hash(&3));
        assert!(matches!(
            decode_canonical(
                &Codec {
                    alias: true,
                    panic_hash: false
                },
                &bytes
            ),
            Err(Error::InvalidFormat(_))
        ));
        assert!(matches!(
            decode_canonical(
                &Codec {
                    alias: false,
                    panic_hash: true
                },
                &bytes
            ),
            Err(Error::InvalidState(_))
        ));
    }
}
