//! 记录占槽编码与页尾填充。零填充仅由明确的页剩余范围解释。
use super::wire::{Reader, checksum, invalid};
use crate::types::*;

pub(crate) const HEADER_BYTES: usize = 48;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecordHeader {
    pub previous: Option<LogAddress>,
    pub version: CheckpointVersion,
    pub key_bytes: u32,
    pub value_bytes: u32,
    /// 值的磁盘槽容量，不包含键、记录头和尾部校验。
    pub capacity_bytes: u32,
    pub tombstone: bool,
    pub invalid: bool,
    pub final_record: bool,
}

impl RecordHeader {
    pub fn encoded_len(&self) -> Result<usize, Error> {
        if self.value_bytes > self.capacity_bytes || (self.tombstone && self.value_bytes != 0) {
            return Err(invalid());
        }
        if let Some(previous) = self.previous {
            previous.validate()?;
        }
        let len = (HEADER_BYTES as u64 + 4)
            .checked_add(u64::from(self.key_bytes))
            .and_then(|n| n.checked_add(u64::from(self.capacity_bytes)))
            .ok_or_else(invalid)?;
        // 单条记录占槽使用 u32，调用者还须约束为当前页可容纳范围。
        usize::try_from(u32::try_from(len).map_err(|_| invalid())?).map_err(|_| invalid())
    }
    pub fn encode(&self, output: &mut [u8]) -> Result<usize, Error> {
        let total = self.encoded_len()? as u32;
        if output.len() != HEADER_BYTES {
            return Err(invalid());
        }
        let mut bytes = Vec::with_capacity(HEADER_BYTES);
        bytes.extend_from_slice(b"RREC");
        bytes.extend_from_slice(&1u16.to_le_bytes());
        let flags = u16::from(self.tombstone)
            | (u16::from(self.invalid) << 1)
            | (u16::from(self.final_record) << 2);
        bytes.extend_from_slice(&flags.to_le_bytes());
        bytes.extend_from_slice(&total.to_le_bytes());
        bytes.extend_from_slice(&self.key_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.value_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.capacity_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.previous.unwrap_or(LogAddress::INVALID).0.to_le_bytes());
        bytes.extend_from_slice(&self.version.0.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&checksum(&bytes).to_le_bytes());
        output.copy_from_slice(&bytes);
        Ok(HEADER_BYTES)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != HEADER_BYTES {
            return Err(invalid());
        }
        let mut r = Reader::new(bytes);
        if r.take(4)? != b"RREC" || r.u16()? != 1 {
            return Err(invalid());
        }
        let flags = r.u16()?;
        if flags & !7 != 0 {
            return Err(invalid());
        }
        let total = r.u32()?;
        let key_bytes = r.u32()?;
        let value_bytes = r.u32()?;
        let capacity_bytes = r.u32()?;
        let previous = r.u64()?;
        let header = Self {
            previous: (previous != u64::MAX).then_some(LogAddress(previous)),
            version: CheckpointVersion(r.u64()?),
            key_bytes,
            value_bytes,
            capacity_bytes,
            tombstone: flags & 1 != 0,
            invalid: flags & 2 != 0,
            final_record: flags & 4 != 0,
        };
        if r.u32()? != 0
            || r.u32()? != checksum(&bytes[..44])
            || header.encoded_len()? != total as usize
        {
            return Err(invalid());
        }
        r.finish()?;
        Ok(header)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Record<'a> {
    pub header: RecordHeader,
    pub key: &'a [u8],
    pub value: &'a [u8],
}
impl Record<'_> {
    pub fn encode(&self, output: &mut [u8]) -> Result<usize, Error> {
        let len = self.header.encoded_len()?;
        if output.len() != len
            || self.key.len() != self.header.key_bytes as usize
            || self.value.len() != self.header.value_bytes as usize
        {
            return Err(invalid());
        }
        self.header.encode(&mut output[..HEADER_BYTES])?;
        let key_end = HEADER_BYTES + self.key.len();
        output[HEADER_BYTES..key_end].copy_from_slice(self.key);
        output[key_end..key_end + self.value.len()].copy_from_slice(self.value);
        output[key_end + self.value.len()..len - 4].fill(0);
        let crc = checksum(&output[..len - 4]);
        output[len - 4..].copy_from_slice(&crc.to_le_bytes());
        Ok(len)
    }
}
impl<'a> Record<'a> {
    /// 输入必须是一个完整占槽，不能包含下一条记录；页遍历者据头部长度切片。
    pub fn decode(bytes: &'a [u8]) -> Result<Self, Error> {
        let header = RecordHeader::decode(bytes.get(..HEADER_BYTES).ok_or_else(invalid)?)?;
        let len = header.encoded_len()?;
        if bytes.len() != len {
            return Err(invalid());
        }
        let key_end = HEADER_BYTES + header.key_bytes as usize;
        let value_end = key_end + header.value_bytes as usize;
        if bytes[value_end..len - 4].iter().any(|&b| b != 0)
            || Reader::new(&bytes[len - 4..]).u32()? != checksum(&bytes[..len - 4])
        {
            return Err(invalid());
        }
        Ok(Self {
            header,
            key: &bytes[HEADER_BYTES..key_end],
            value: &bytes[key_end..value_end],
        })
    }
}

/// 页尾不足一个头部，或上层明确结束该页时，用零填满剩余范围。
#[cfg(test)]
pub(crate) fn encode_padding(remaining_page: &mut [u8]) {
    remaining_page.fill(0);
}
#[cfg(test)]
pub(crate) fn validate_padding(remaining_page: &[u8]) -> Result<(), Error> {
    if remaining_page.iter().all(|&b| b == 0) {
        Ok(())
    } else {
        Err(invalid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Vec<u8> {
        let text = include_str!("../../tests/fixtures/p1-record.hex").trim();
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn 固定记录样例解码并逐字节复原() {
        let bytes = fixture();
        let record = Record::decode(&bytes).unwrap();
        assert_eq!(record.key, b"k\xff");
        assert_eq!(record.value, b"abc");
        assert_eq!(record.header.version, CheckpointVersion(7));
        assert_eq!(record.header.previous, None);
        assert_eq!(record.header.capacity_bytes, 4);
        let mut output = vec![0xaa; bytes.len()];
        record.encode(&mut output).unwrap();
        assert_eq!(bytes, output);
        assert_eq!(checksum(b"123456789"), 0xe3069283);
    }

    #[test]
    fn 截断追加和每一位损坏均拒绝() {
        let bytes = fixture();
        for len in 0..bytes.len() {
            assert!(Record::decode(&bytes[..len]).is_err());
        }
        let mut extended = bytes.clone();
        extended.push(0);
        assert!(Record::decode(&extended).is_err());
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut bad = bytes.clone();
                bad[i] ^= 1 << bit;
                assert!(Record::decode(&bad).is_err(), "偏移 {i} 位 {bit}");
            }
        }
    }

    fn repair(bytes: &mut [u8]) {
        let header_crc = checksum(&bytes[..44]);
        bytes[44..48].copy_from_slice(&header_crc.to_le_bytes());
        let len = bytes.len();
        let crc = checksum(&bytes[..len - 4]);
        bytes[len - 4..].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn 重算校验也不能绕过版本长度与保留位校验() {
        for (offset, data) in [
            (4, vec![2, 0]),
            (6, vec![8, 0]),
            (8, 57u32.to_le_bytes().to_vec()),
            (12, u32::MAX.to_le_bytes().to_vec()),
            (16, 5u32.to_le_bytes().to_vec()),
            (40, vec![1]),
            (53, vec![1]),
        ] {
            let mut bytes = fixture();
            bytes[offset..offset + data.len()].copy_from_slice(&data);
            repair(&mut bytes);
            assert!(Record::decode(&bytes).is_err(), "偏移 {offset}");
        }
    }

    #[test]
    fn 墓碑空键无效记录与页末标记可往返() {
        let header = RecordHeader {
            previous: Some(LogAddress(0)),
            version: CheckpointVersion(u64::MAX),
            key_bytes: 0,
            value_bytes: 0,
            capacity_bytes: 0,
            tombstone: true,
            invalid: true,
            final_record: true,
        };
        let record = Record {
            header,
            key: b"",
            value: b"",
        };
        let mut bytes = vec![0; record.header.encoded_len().unwrap()];
        record.encode(&mut bytes).unwrap();
        assert_eq!(Record::decode(&bytes).unwrap(), record);
        let mut bad = record.header.clone();
        bad.value_bytes = 1;
        bad.capacity_bytes = 1;
        assert!(bad.encoded_len().is_err());
        bad = record.header.clone();
        bad.previous = Some(LogAddress::INVALID);
        assert!(bad.encoded_len().is_err());
    }

    #[test]
    fn 拒绝不修改输出且页填充不能伪装为记录() {
        let bytes = fixture();
        let mut record = Record::decode(&bytes).unwrap();
        let mut output = vec![0xaa; bytes.len()];
        record.value = b"x";
        assert!(record.encode(&mut output).is_err());
        assert!(output.iter().all(|&b| b == 0xaa));
        for len in [0, 1, 47, 48, 4096] {
            let mut pad = vec![0xaa; len];
            encode_padding(&mut pad);
            validate_padding(&pad).unwrap();
            assert!(Record::decode(&pad).is_err());
            if len > 0 {
                pad[len - 1] = 1;
                assert!(validate_padding(&pad).is_err());
            }
        }
    }
}
