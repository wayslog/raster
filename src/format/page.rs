//! 页帧包含完整逻辑页和整体校验；零间隙不能绕过页级损坏检查。
use super::{
    HEADER_BYTES, Record, RecordHeader,
    wire::{checksum, invalid},
};
use crate::types::*;
const PREFIX: usize = 32;
const OVERHEAD: usize = PREFIX + 4;

pub(crate) struct PageFrame<'a> {
    pub page: PageId,
    pub version: CheckpointVersion,
    pub payload: &'a [u8],
}
impl PageFrame<'_> {
    pub fn encoded_size(page_bytes: usize) -> Result<usize, Error> {
        if !page_bytes.is_power_of_two() || u32::try_from(page_bytes).is_err() {
            return Err(invalid());
        }
        page_bytes
            .checked_add(OVERHEAD)
            .ok_or(Error::CapacityExceeded)
    }
    /// 物理偏移独立于日志地址；每页都计入固定帧头与尾部校验。
    pub fn physical_offset(page: PageId, page_bytes: usize) -> Result<u64, Error> {
        if !page_bytes.is_power_of_two() {
            return Err(invalid());
        }
        let stride = u64::try_from(
            page_bytes
                .checked_add(OVERHEAD)
                .ok_or(Error::CapacityExceeded)?,
        )
        .map_err(|_| Error::CapacityExceeded)?;
        page.0.checked_mul(stride).ok_or(Error::CapacityExceeded)
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.records()?;
        let len = u32::try_from(self.payload.len()).map_err(|_| Error::CapacityExceeded)?;
        let total = self
            .payload
            .len()
            .checked_add(OVERHEAD)
            .ok_or(Error::CapacityExceeded)?;
        let mut out = Vec::new();
        out.try_reserve_exact(total)
            .map_err(|_| Error::OutOfMemory)?;
        out.extend_from_slice(b"RPAG");
        out.extend_from_slice(&1u16.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&self.page.0.to_le_bytes());
        out.extend_from_slice(&self.version.0.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&checksum(&out).to_le_bytes());
        out.extend_from_slice(self.payload);
        out.extend_from_slice(&checksum(&out).to_le_bytes());
        Ok(out)
    }
    pub fn records(&self) -> Result<Vec<(LogAddress, Record<'_>)>, Error> {
        if !self.payload.len().is_power_of_two() {
            return Err(invalid());
        }
        let base = LogAddress::from_page_offset(self.page, 0, self.payload.len() as u64)?;
        base.checked_add(self.payload.len() as u64)?;
        let mut records = Vec::new();
        let mut at = 0;
        while at < self.payload.len() {
            if self.payload[at] == 0 {
                at += 1;
                continue;
            }
            let header_end = at.checked_add(HEADER_BYTES).ok_or_else(invalid)?;
            let header =
                RecordHeader::decode(self.payload.get(at..header_end).ok_or_else(invalid)?)?;
            let end = at.checked_add(header.encoded_len()?).ok_or_else(invalid)?;
            let record = Record::decode(self.payload.get(at..end).ok_or_else(invalid)?)?;
            let address = base.checked_add(at as u64)?;
            if record
                .header
                .previous
                .is_some_and(|previous| previous >= address)
                || record.header.version > self.version
            {
                return Err(invalid());
            }
            records.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
            records.push((address, record));
            at = end;
        }
        Ok(records)
    }
}
impl<'a> PageFrame<'a> {
    pub fn decode(
        bytes: &'a [u8],
        expected_page: PageId,
        page_bytes: usize,
    ) -> Result<Self, Error> {
        if bytes.len() != page_bytes.checked_add(OVERHEAD).ok_or_else(invalid)?
            || !page_bytes.is_power_of_two()
            || bytes.len() < OVERHEAD
        {
            return Err(invalid());
        }
        if &bytes[..4] != b"RPAG"
            || bytes[4..8] != [1, 0, 0, 0]
            || u32::from_le_bytes(bytes[24..28].try_into().expect("固定头部")) as usize
                != page_bytes
            || u32::from_le_bytes(bytes[28..32].try_into().expect("固定头部"))
                != checksum(&bytes[..28])
            || u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().expect("固定尾部"))
                != checksum(&bytes[..bytes.len() - 4])
        {
            return Err(invalid());
        }
        let frame = Self {
            page: PageId(u64::from_le_bytes(
                bytes[8..16].try_into().expect("固定头部"),
            )),
            version: CheckpointVersion(u64::from_le_bytes(
                bytes[16..24].try_into().expect("固定头部"),
            )),
            payload: &bytes[PREFIX..bytes.len() - 4],
        };
        if frame.page != expected_page {
            return Err(invalid());
        }
        frame.records()?;
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn 页帧间隙可遍历且整条记录被清零也会拒绝() {
        let record = Record {
            header: RecordHeader {
                previous: None,
                version: CheckpointVersion(2),
                key_bytes: 1,
                value_bytes: 1,
                capacity_bytes: 1,
                tombstone: false,
                invalid: false,
                final_record: false,
            },
            key: b"k",
            value: b"v",
        };
        let mut payload = [0; 256];
        record.encode(&mut payload[8..62]).unwrap();
        record.encode(&mut payload[80..134]).unwrap();
        let frame = PageFrame {
            page: PageId(3),
            version: CheckpointVersion(2),
            payload: &payload,
        };
        let bytes = frame.encode().unwrap();
        let decoded = PageFrame::decode(&bytes, PageId(3), 256).unwrap();
        let records = decoded.records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, LogAddress(776));
        assert_eq!(records[1].0, LogAddress(848));
        let mut corrupt = bytes.clone();
        corrupt[PREFIX + 8..PREFIX + 62].fill(0);
        assert!(PageFrame::decode(&corrupt, PageId(3), 256).is_err());
        assert!(PageFrame::decode(&bytes, PageId(4), 256).is_err());
        for end in 0..bytes.len() {
            assert!(PageFrame::decode(&bytes[..end], PageId(3), 256).is_err());
        }
        assert_eq!(PageFrame::physical_offset(PageId(3), 256).unwrap(), 876);
        assert!(PageFrame::physical_offset(PageId(u64::MAX), 256).is_err());
    }
}
