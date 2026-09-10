//! 索引模糊快照的显式编码；进程内 owner/revision 和缓存头不能进入磁盘映像。
use super::wire::{Reader, checksum, invalid};
use crate::types::*;
const HEADER: usize = 32;
const ENTRY: usize = 24;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub bucket: u64,
    pub tag: u16,
    pub address: LogAddress,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexSnapshot {
    pub buckets: u64,
    pub generation: Generation,
    pub entries: Vec<IndexEntry>,
}
impl IndexSnapshot {
    pub fn validate(&self) -> Result<(), Error> {
        if !self.buckets.is_power_of_two() {
            return Err(invalid());
        }
        let mut previous = None;
        for entry in &self.entries {
            entry.address.validate()?;
            let key = (entry.bucket, entry.tag);
            if entry.bucket >= self.buckets || previous.is_some_and(|old| old >= key) {
                return Err(invalid());
            }
            previous = Some(key);
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let length = self
            .entries
            .len()
            .checked_mul(ENTRY)
            .and_then(|n| n.checked_add(HEADER + 4))
            .ok_or(Error::CapacityExceeded)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| Error::OutOfMemory)?;
        bytes.extend_from_slice(b"RIND\x01\x00\x00\x00");
        bytes.extend_from_slice(&self.buckets.to_le_bytes());
        bytes.extend_from_slice(&self.generation.0.to_le_bytes());
        bytes.extend_from_slice(&(self.entries.len() as u64).to_le_bytes());
        for entry in &self.entries {
            bytes.extend_from_slice(&entry.bucket.to_le_bytes());
            bytes.extend_from_slice(&entry.tag.to_le_bytes());
            bytes.extend_from_slice(&[0; 6]);
            bytes.extend_from_slice(&entry.address.0.to_le_bytes());
        }
        let crc = checksum(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < HEADER + 4 {
            return Err(invalid());
        }
        let (payload, tail) = bytes.split_at(bytes.len() - 4);
        if checksum(payload) != u32::from_le_bytes(tail.try_into().map_err(|_| invalid())?) {
            return Err(invalid());
        }
        let mut reader = Reader::new(payload);
        if reader.take(4)? != b"RIND" || reader.u16()? != 1 || reader.u16()? != 0 {
            return Err(invalid());
        }
        let buckets = reader.u64()?;
        let generation = Generation(reader.u64()?);
        let count = usize::try_from(reader.u64()?).map_err(|_| invalid())?;
        if count.checked_mul(ENTRY).and_then(|n| n.checked_add(HEADER)) != Some(payload.len()) {
            return Err(invalid());
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| Error::OutOfMemory)?;
        for _ in 0..count {
            let bucket = reader.u64()?;
            let tag = reader.u16()?;
            if reader.take(6)? != [0; 6] {
                return Err(invalid());
            }
            entries.push(IndexEntry {
                bucket,
                tag,
                address: LogAddress(reader.u64()?),
            });
        }
        reader.finish()?;
        let result = Self {
            buckets,
            generation,
            entries,
        };
        result.validate()?;
        Ok(result)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> Vec<u8> {
        let text = include_str!("../../tests/fixtures/p5-index.hex").trim();
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }
    fn repair(bytes: &mut [u8]) {
        let n = bytes.len() - 4;
        let crc = checksum(&bytes[..n]);
        bytes[n..].copy_from_slice(&crc.to_le_bytes());
    }
    #[test]
    fn 固定索引样例精确解码和编码且保留零地址与标签边界() {
        let bytes = fixture();
        let image = IndexSnapshot::decode(&bytes).unwrap();
        assert_eq!(image.buckets, 8);
        assert_eq!(image.generation, Generation(3));
        assert_eq!(
            image.entries,
            vec![
                IndexEntry {
                    bucket: 0,
                    tag: 7,
                    address: LogAddress(0)
                },
                IndexEntry {
                    bucket: 7,
                    tag: u16::MAX,
                    address: LogAddress(4096)
                }
            ]
        );
        assert_eq!(image.encode().unwrap(), bytes);
        let empty = IndexSnapshot {
            buckets: 1,
            generation: Generation(0),
            entries: vec![],
        };
        assert_eq!(empty.encode().unwrap().len(), 36);
        assert_eq!(
            IndexSnapshot::decode(&empty.encode().unwrap()).unwrap(),
            empty
        );
    }
    #[test]
    fn 截断尾随和逐位损坏拒绝且计数溢出不分配() {
        let bytes = fixture();
        for end in 0..bytes.len() {
            assert!(IndexSnapshot::decode(&bytes[..end]).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(IndexSnapshot::decode(&extra).is_err());
        for bit in 0..bytes.len() * 8 {
            let mut bad = bytes.clone();
            bad[bit / 8] ^= 1 << (bit % 8);
            assert!(IndexSnapshot::decode(&bad).is_err());
        }
        let mut bad = bytes;
        bad[24..32].copy_from_slice(&u64::MAX.to_le_bytes());
        repair(&mut bad);
        assert!(IndexSnapshot::decode(&bad).is_err());
    }
    #[test]
    fn 重算校验后仍拒绝未知版本保留位重复桶标签和非法地址() {
        let bytes = fixture();
        for offset in [4, 6, 42, 43, 44, 45, 46, 47] {
            let mut bad = bytes.clone();
            bad[offset] = 2;
            repair(&mut bad);
            assert!(IndexSnapshot::decode(&bad).is_err());
        }
        for buckets in [0u64, 7] {
            let mut bad = bytes.clone();
            bad[8..16].copy_from_slice(&buckets.to_le_bytes());
            repair(&mut bad);
            assert!(IndexSnapshot::decode(&bad).is_err());
        }
        let mut bad = bytes.clone();
        bad[56..64].copy_from_slice(&8u64.to_le_bytes());
        repair(&mut bad);
        assert!(IndexSnapshot::decode(&bad).is_err());
        let mut bad = bytes.clone();
        let first = bad[32..56].to_vec();
        bad[56..80].copy_from_slice(&first);
        repair(&mut bad);
        assert!(IndexSnapshot::decode(&bad).is_err());
        let mut bad = bytes.clone();
        bad[48..56].copy_from_slice(&u64::MAX.to_le_bytes());
        repair(&mut bad);
        assert!(IndexSnapshot::decode(&bad).is_err());
        let mut bad = bytes;
        let first = bad[32..56].to_vec();
        let second = bad[56..80].to_vec();
        bad[32..56].copy_from_slice(&second);
        bad[56..80].copy_from_slice(&first);
        repair(&mut bad);
        assert!(IndexSnapshot::decode(&bad).is_err());
    }
}
