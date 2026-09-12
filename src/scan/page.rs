//! Retain an owned disk page after a full checksum;The index only records the slot range,Do not construct user values.
use crate::{format::PageFrame, types::*};
use std::ops::Range;

pub(crate) struct PageCursor {
    bytes: Vec<u8>,
    records: Vec<(LogAddress, Range<usize>)>,
    next: usize,
    position: LogAddress,
}
impl PageCursor {
    /// begin/end Is the half-open range within the current page,Allow pointing to padding;Records cannot be cut off in non-empty ranges.
    /// Validate entire input page even if empty range is requested,Cannot use range selection to bypass corruption checking.
    pub fn new(
        bytes: Vec<u8>,
        page: PageId,
        page_bytes: usize,
        begin: LogAddress,
        end: LogAddress,
    ) -> Result<Self, Error> {
        begin.validate()?;
        end.validate()?;
        let frame = PageFrame::decode(&bytes, page, page_bytes)?;
        let base = LogAddress::from_page_offset(page, 0, page_bytes as u64)?;
        let limit = base.checked_add(page_bytes as u64)?;
        if begin < base || begin > end || end > limit {
            return Err(Error::InvalidFormat("Page scan range is invalid"));
        }
        // The entire frame consists of a fixed prefix,Logical page and four-byte checksum;The slice still references the same owning buffer.
        let prefix = bytes.len() - frame.payload.len() - 4;
        let mut records = Vec::new();
        for (address, record) in frame.records()? {
            let length = record.header.encoded_len()?;
            let record_end = address.checked_add(length as u64)?;
            if begin != end
                && [begin, end]
                    .into_iter()
                    .any(|boundary| address < boundary && boundary < record_end)
            {
                return Err(Error::InvalidFormat(
                    "Page scan boundary is in the middle of the record",
                ));
            }
            if begin <= address && address < end {
                let offset =
                    usize::try_from(address.0 - base.0).map_err(|_| Error::CapacityExceeded)?;
                records.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
                records.push((address, prefix + offset..prefix + offset + length));
            }
        }
        Ok(Self {
            bytes,
            records,
            next: 0,
            position: begin,
        })
    }

    /// Schema Decoding failure does not consume slots;The scan driver decides to retry or fail based on the impact of the error..
    pub fn next_record<S: crate::schema::Schema>(
        &mut self,
        schema: &S,
    ) -> Result<Option<crate::api::scan::ScannedRecord<S>>, Error> {
        let Some((address, range)) = self.records.get(self.next) else {
            return Ok(None);
        };
        let record = super::record::decode_record(schema, *address, &self.bytes[range.clone()])?;
        self.position = address.checked_add(range.len() as u64)?;
        self.next += 1;
        Ok(Some(record))
    }

    pub fn position(&self) -> LogAddress {
        self.position
    }

    /// Only complete copies are allocated successfully to advance.The return value is independent of the cursor,Can be reused across pages and retained after buffering.
    pub fn next_encoded(&mut self) -> Result<Option<(LogAddress, Vec<u8>)>, Error> {
        let Some((address, range)) = self.records.get(self.next) else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(range.len())
            .map_err(|_| Error::OutOfMemory)?;
        bytes.extend_from_slice(&self.bytes[range.clone()]);
        self.next += 1;
        Ok(Some((*address, bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Record, RecordHeader};

    fn fixture() -> Vec<u8> {
        let text = include_str!("../../tests/fixtures/p5-page.hex").trim();
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }
    fn cursor(bytes: Vec<u8>, begin: u64, end: u64) -> Result<PageCursor, Error> {
        PageCursor::new(bytes, PageId(2), 256, LogAddress(begin), LogAddress(end))
    }
    #[test]
    fn fixed_page_scan_returns_to_independent_slot_and_repeat_end_is_stable() {
        let mut scan = cursor(fixture(), 512, 768).unwrap();
        let (address, bytes) = scan.next_encoded().unwrap().unwrap();
        assert_eq!(address, LogAddress(528));
        assert_eq!(Record::decode(&bytes).unwrap().value, b"abc");
        assert!(scan.next_encoded().unwrap().is_none());
        assert!(scan.next_encoded().unwrap().is_none());
        drop(scan);
        let record = Record::decode(&bytes).unwrap();
        assert_eq!(record.key, [b'k', 255]);
        assert_eq!(record.header.version, CheckpointVersion(7));
    }
    #[test]
    fn in_page_ranges_reject_cutoff_slots_and_empty_ranges_and_padding_do_not_generate_records() {
        for (begin, end) in [
            (511, 768),
            (512, 769),
            (600, 512),
            (529, 768),
            (512, 529),
            (585, 768),
        ] {
            assert!(cursor(fixture(), begin, end).is_err(), "{begin}..{end}");
        }
        for (begin, end) in [(512, 528), (586, 768), (529, 529), (768, 768)] {
            assert!(
                cursor(fixture(), begin, end)
                    .unwrap()
                    .next_encoded()
                    .unwrap()
                    .is_none()
            );
        }
        let mut scan = cursor(fixture(), 528, 586).unwrap();
        assert_eq!(scan.next_encoded().unwrap().unwrap().0, LogAddress(528));
        assert!(scan.next_encoded().unwrap().is_none());
        assert!(cursor(fixture(), u64::MAX, u64::MAX).is_err());
    }
    #[test]
    fn out_of_range_corruption_and_incorrect_page_number_truncation_cannot_bypass_verification() {
        let bytes = fixture();
        for offset in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[offset] ^= 1;
            assert!(cursor(corrupt, 768, 768).is_err(), "offset {offset}");
        }
        for end in 0..bytes.len() {
            assert!(cursor(bytes[..end].to_vec(), 512, 768).is_err());
        }
        assert!(
            PageCursor::new(
                bytes.clone(),
                PageId(1),
                256,
                LogAddress(256),
                LogAddress(512)
            )
            .is_err()
        );
        assert!(PageCursor::new(bytes, PageId(2), 128, LogAddress(256), LogAddress(384)).is_err());
    }
    #[test]
    fn the_address_sequence_retains_the_same_key_old_version_tombstone_invalid_records_and_variable_length_slots()
     {
        let mut payload = vec![0; 512];
        let mut previous = None;
        for (offset, version, value, tombstone, invalid, final_record) in [
            (8, 1, &b"old"[..], false, false, false),
            (88, 2, &b"new-value"[..], false, false, false),
            (192, 3, &b""[..], true, false, false),
            (280, 4, &b"uncommitted"[..], false, true, true),
        ] {
            let record = Record {
                header: RecordHeader {
                    previous,
                    version: CheckpointVersion(version),
                    key_bytes: 2,
                    value_bytes: value.len() as u32,
                    capacity_bytes: value.len() as u32 + 7,
                    tombstone,
                    invalid,
                    final_record,
                },
                key: &[0, 255],
                value,
            };
            let length = record.header.encoded_len().unwrap();
            record
                .encode(&mut payload[offset..offset + length])
                .unwrap();
            previous = Some(LogAddress(512 + offset as u64));
        }
        let bytes = PageFrame {
            page: PageId(1),
            version: CheckpointVersion(4),
            payload: &payload,
        }
        .encode()
        .unwrap();
        let mut scan =
            PageCursor::new(bytes, PageId(1), 512, LogAddress(512), LogAddress(1024)).unwrap();
        for (offset, version, tombstone, invalid) in [
            (8, 1, false, false),
            (88, 2, false, false),
            (192, 3, true, false),
            (280, 4, false, true),
        ] {
            let (address, bytes) = scan.next_encoded().unwrap().unwrap();
            let record = Record::decode(&bytes).unwrap();
            assert_eq!(address, LogAddress(512 + offset));
            assert_eq!(record.header.version, CheckpointVersion(version));
            assert_eq!(
                (record.header.tombstone, record.header.invalid),
                (tombstone, invalid)
            );
            assert_eq!(record.key, [0, 255]);
        }
        assert!(scan.next_encoded().unwrap().is_none());
    }
}

#[cfg(test)]
mod typed_tests {
    use super::*;
    use crate::schema::builtin::{ByteKey, ByteValueCodec, SchemaPair, SerializedValue, U64Key};
    #[test]
    fn in_page_type_decoding_fails_no_records_are_consumed_and_the_page_can_be_released_after_returning()
     {
        let text = include_str!("../../tests/fixtures/p5-page.hex").trim();
        let bytes = (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect();
        let mut cursor =
            PageCursor::new(bytes, PageId(2), 256, LogAddress(512), LogAddress(768)).unwrap();
        let wrong = SchemaPair::new(U64Key, SerializedValue::new(ByteValueCodec));
        assert!(cursor.next_record(&wrong).is_err());
        let schema = SchemaPair::new(ByteKey, SerializedValue::new(ByteValueCodec));
        let result = cursor.next_record(&schema).unwrap().unwrap();
        assert!(cursor.next_record(&schema).unwrap().is_none());
        drop(cursor);
        assert_eq!(result.address, LogAddress(528));
        assert_eq!(result.key, [b'k', 255]);
        assert_eq!(result.value, Some(b"abc".to_vec()));
    }
}
