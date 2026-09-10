//! 稳定哈希与显式键编码；不能用默认 Hash 的平台相关输入冻结格式。
use crate::types::{Error, FormatId, HashDescriptor, KeyHash};
use std::borrow::Borrow;

/// 编码是规范字节表示；相等的键必须具有相同编码和哈希。
/// 编码失败不得部分修改输出，输出长度必须恰好匹配 encoded_len。
/// 编码版本、算法或种子变化必须在读取记录前拒绝；不能静默重建索引掩盖变化。
pub trait KeyCodec: Send + Sync + 'static {
    type Key: ?Sized;
    /// 拥有型键必须能借用为相同逻辑键，供恢复和扫描重新计算稳定哈希。
    type OwnedKey: Borrow<Self::Key> + 'static;
    fn format_id(&self) -> FormatId;
    fn hash_descriptor(&self) -> HashDescriptor;
    fn hash(&self, key: &Self::Key) -> KeyHash;
    fn encoded_len(&self, key: &Self::Key) -> Result<u32, Error>;
    fn encode(&self, key: &Self::Key, output: &mut [u8]) -> Result<(), Error>;
    fn equals_encoded(&self, key: &Self::Key, encoded: &[u8]) -> Result<bool, Error>;
    fn decode_owned(&self, encoded: &[u8]) -> Result<Self::OwnedKey, Error>;

    /// 检查持久材料声明的键语义；材料解析及恢复入口由后续任务接入。
    fn validate_identity(&self, format: FormatId, hash: &HashDescriptor) -> Result<(), Error> {
        if self.format_id() != format {
            return Err(Error::InvalidFormat("键编码版本不匹配"));
        }
        if &self.hash_descriptor() != hash {
            return Err(Error::InvalidFormat("键哈希算法或种子不匹配"));
        }
        Ok(())
    }
}

/// 从持久字节取得规范键与完整哈希；结果只能用于已经验证过 Schema 身份的材料。
pub(crate) fn decode_canonical<K: KeyCodec>(
    codec: &K,
    encoded: &[u8],
) -> Result<(K::OwnedKey, KeyHash), Error> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let owned = codec.decode_owned(encoded)?;
        let key = owned.borrow();
        if codec.encoded_len(key)? as usize != encoded.len() {
            return Err(Error::InvalidFormat("解码键长度不符合规范编码"));
        }
        let mut canonical = Vec::new();
        canonical
            .try_reserve_exact(encoded.len())
            .map_err(|_| Error::OutOfMemory)?;
        canonical.resize(encoded.len(), 0);
        codec.encode(key, &mut canonical)?;
        if canonical != encoded || !codec.equals_encoded(key, encoded)? {
            return Err(Error::InvalidFormat("恢复键不是规范编码"));
        }
        let hash = codec.hash(key);
        Ok((owned, hash))
    }))
    .map_err(|_| Error::InvalidState("恢复键解码或哈希恐慌"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::builtin::{ByteKey, U64Key};
    #[test]
    fn 恢复键规范往返得到完整稳定哈希() {
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
            assert!(!self.panic_hash, "注入恢复哈希恐慌");
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
    fn 自定义拥有键可借用且非规范别名和哈希恐慌拒绝() {
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
