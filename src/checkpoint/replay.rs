//! 按地址重放已验证的页，过滤版本并修复跨越被过滤记录的前驱。
use crate::{
    format::{IndexEntry, IndexSnapshot, PageFrame, Record},
    schema::{KeyCodec, key::decode_canonical},
    types::*,
};
use std::collections::BTreeMap;

pub(crate) struct ReplayOptions {
    pub begin: LogAddress,
    pub end: LogAddress,
    pub page_bytes: usize,
    pub version: CheckpointVersion,
    pub buckets: usize,
    pub generation: Generation,
    /// 限制本次重放保存的源记录链接数量；达到上限返回错误而不扩展预算。
    pub max_records: usize,
}
#[derive(Clone, Copy)]
struct Link {
    tag: u16,
    retained: Option<LogAddress>,
}
pub(crate) struct Replay {
    options: ReplayOptions,
    next_page: u64,
    links: BTreeMap<LogAddress, Link>,
    heads: BTreeMap<(u64, u16), LogAddress>,
}
impl Replay {
    pub fn new(options: ReplayOptions) -> Result<Self, Error> {
        options.begin.validate()?;
        options.end.validate()?;
        PageFrame::encoded_size(options.page_bytes)?;
        if options.begin > options.end
            || !options.end.0.is_multiple_of(options.page_bytes as u64)
            || !options.buckets.is_power_of_two()
        {
            return Err(Error::InvalidFormat("重放范围或桶配置无效"));
        }
        Ok(Self {
            next_page: options.begin.0 / options.page_bytes as u64,
            options,
            links: BTreeMap::new(),
            heads: BTreeMap::new(),
        })
    }
    /// 整页校验和编码成功后才推进状态；返回页供调用者写入独立恢复日志。
    pub fn page<K: KeyCodec>(&mut self, bytes: &[u8], codec: &K) -> Result<Vec<u8>, Error> {
        if self.next_page >= self.options.end.0 / self.options.page_bytes as u64 {
            return Err(Error::InvalidState("重放范围已结束"));
        }
        let frame = PageFrame::decode(bytes, PageId(self.next_page), self.options.page_bytes)?;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(self.options.page_bytes)
            .map_err(|_| Error::OutOfMemory)?;
        payload.resize(self.options.page_bytes, 0);
        let mut links = BTreeMap::<LogAddress, Link>::new();
        let mut heads = BTreeMap::new();
        for (address, record) in frame.records()? {
            if address < self.options.begin || record.header.invalid {
                continue;
            }
            if self
                .links
                .len()
                .checked_add(links.len())
                .is_none_or(|n| n >= self.options.max_records)
            {
                return Err(Error::CapacityExceeded);
            }
            let (_, hash) = decode_canonical(codec, record.key)?;
            let tag = hash.tag();
            let previous = match record.header.previous {
                None => None,
                Some(previous) if previous < self.options.begin => None,
                Some(previous) => {
                    let link = links
                        .get(&previous)
                        .or_else(|| self.links.get(&previous))
                        .ok_or(Error::InvalidFormat("日志前驱不是已验证的记录"))?;
                    // 历史索引增长可改变桶号，但同一前驱链的 tag 必须相同。
                    if link.tag != tag {
                        return Err(Error::InvalidFormat("日志前驱标签不匹配"));
                    }
                    link.retained
                }
            };
            let retained = if record.header.version <= self.options.version {
                let mut header = record.header.clone();
                header.previous = previous;
                let length = header.encoded_len()?;
                let offset = (address.0 % self.options.page_bytes as u64) as usize;
                Record {
                    header,
                    key: record.key,
                    value: record.value,
                }
                .encode(&mut payload[offset..offset + length])?;
                heads.insert((hash.0 & (self.options.buckets as u64 - 1), tag), address);
                Some(address)
            } else {
                previous
            };
            links.insert(address, Link { tag, retained });
        }
        let bytes = PageFrame {
            page: frame.page,
            version: self.options.version,
            payload: &payload,
        }
        .encode()?;
        self.links.extend(links);
        self.heads.extend(heads);
        self.next_page += 1;
        Ok(bytes)
    }
    pub fn complete(&self) -> bool {
        self.next_page == self.options.end.0 / self.options.page_bytes as u64
    }
    /// 用于核对模糊索引的原链头；新版本链头可能解析为旧地址或空链。
    pub fn resolve_head(&self, address: LogAddress, tag: u16) -> Result<Option<LogAddress>, Error> {
        let link = self
            .links
            .get(&address)
            .ok_or(Error::InvalidFormat("索引链头不是重放记录"))?;
        if link.tag != tag {
            return Err(Error::InvalidFormat("索引链头标签与记录不一致"));
        }
        Ok(link.retained)
    }
    pub fn index(&self) -> Result<IndexSnapshot, Error> {
        if !self.complete() {
            return Err(Error::InvalidState("日志重放尚未完成"));
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(self.heads.len())
            .map_err(|_| Error::OutOfMemory)?;
        entries.extend(
            self.heads
                .iter()
                .map(|(&(bucket, tag), &address)| IndexEntry {
                    bucket,
                    tag,
                    address,
                }),
        );
        Ok(IndexSnapshot {
            buckets: self.options.buckets as u64,
            generation: self.options.generation,
            entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{format::RecordHeader, schema::builtin::U64Key};
    fn options(end: u64, max_records: usize) -> ReplayOptions {
        ReplayOptions {
            begin: LogAddress(0),
            end: LogAddress(end),
            page_bytes: 256,
            version: CheckpointVersion(0),
            buckets: 1,
            generation: Generation(3),
            max_records,
        }
    }
    type TestRecord = (usize, u64, u64, Option<u64>, bool, bool);
    fn page(number: u64, records: &[TestRecord]) -> Vec<u8> {
        let mut payload = vec![0; 256];
        for &(offset, key, version, previous, tombstone, invalid) in records {
            let key = key.to_le_bytes();
            let value = 17u64.to_le_bytes();
            let header = RecordHeader {
                previous: previous.map(LogAddress),
                version: CheckpointVersion(version),
                key_bytes: 8,
                value_bytes: if tombstone { 0 } else { 8 },
                capacity_bytes: 8,
                tombstone,
                invalid,
                final_record: false,
            };
            let length = header.encoded_len().unwrap();
            Record {
                header,
                key: &key,
                value: if tombstone { &[] } else { &value },
            }
            .encode(&mut payload[offset..offset + length])
            .unwrap();
        }
        PageFrame {
            page: PageId(number),
            version: CheckpointVersion(9),
            payload: &payload,
        }
        .encode()
        .unwrap()
    }
    #[test]
    fn 同页过滤新版本并修复旧键经过碰撞新键的链() {
        assert_eq!(U64Key.hash(&8969).tag(), U64Key.hash(&9239).tag());
        let bytes = page(
            0,
            &[
                (0, 8969, 0, None, false, false),
                (80, 9239, 1, Some(0), false, false),
                (160, 8969, 0, Some(80), false, false),
            ],
        );
        let mut replay = Replay::new(options(256, 3)).unwrap();
        let output = replay.page(&bytes, &U64Key).unwrap();
        let frame = PageFrame::decode(&output, PageId(0), 256).unwrap();
        let records = frame.records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, LogAddress(0));
        assert_eq!(records[1].0, LogAddress(160));
        assert_eq!(records[1].1.header.previous, Some(LogAddress(0)));
        assert!(frame.payload[80..148].iter().all(|b| *b == 0));
        assert_eq!(frame.version, CheckpointVersion(0));
        assert_eq!(
            replay
                .resolve_head(LogAddress(80), U64Key.hash(&9239).tag())
                .unwrap(),
            Some(LogAddress(0))
        );
        let index = replay.index().unwrap();
        assert_eq!(index.generation, Generation(3));
        assert_eq!(index.entries.len(), 1);
        assert_eq!(index.entries[0].address, LogAddress(160));
        assert!(replay.page(&bytes, &U64Key).is_err());
    }
    #[test]
    fn 跨页修复并保留旧版本墓碑而过滤新墓碑() {
        let mut replay = Replay::new(options(768, 3)).unwrap();
        replay
            .page(&page(0, &[(0, 8969, 0, None, false, false)]), &U64Key)
            .unwrap();
        assert!(replay.index().is_err());
        let second = replay
            .page(&page(1, &[(0, 9239, 1, Some(0), true, false)]), &U64Key)
            .unwrap();
        assert!(
            PageFrame::decode(&second, PageId(1), 256)
                .unwrap()
                .records()
                .unwrap()
                .is_empty()
        );
        let third = replay
            .page(&page(2, &[(0, 8969, 0, Some(256), true, false)]), &U64Key)
            .unwrap();
        let frame = PageFrame::decode(&third, PageId(2), 256).unwrap();
        let records = frame.records().unwrap();
        assert!(records[0].1.header.tombstone);
        assert!(records[0].1.value.is_empty());
        assert_eq!(records[0].1.header.previous, Some(LogAddress(0)));
        assert_eq!(replay.index().unwrap().entries[0].address, LogAddress(512));
    }
    #[test]
    fn 错页悬空前驱标签错误和预算失败均不推进页状态() {
        let mut replay = Replay::new(options(512, 2)).unwrap();
        assert!(replay.page(&page(1, &[]), &U64Key).is_err());
        replay
            .page(&page(0, &[(0, 8969, 0, None, false, false)]), &U64Key)
            .unwrap();
        for bad in [
            page(1, &[(0, 8969, 0, Some(8), false, false)]),
            page(1, &[(0, 1, 0, Some(0), false, false)]),
            page(
                1,
                &[
                    (0, 8969, 0, Some(0), false, false),
                    (80, 8969, 0, Some(256), false, false),
                ],
            ),
        ] {
            assert!(replay.page(&bad, &U64Key).is_err());
            assert!(!replay.complete());
            assert!(
                replay
                    .resolve_head(LogAddress(256), U64Key.hash(&8969).tag())
                    .is_err()
            );
        }
        replay
            .page(&page(1, &[(0, 8969, 0, Some(0), false, false)]), &U64Key)
            .unwrap();
        assert!(replay.complete());
        assert!(
            replay
                .resolve_head(LogAddress(0), U64Key.hash(&1).tag())
                .is_err()
        );
    }
    #[test]
    fn 截断前驱归空且无效记录不能成为后续链头() {
        let mut config = options(512, 1);
        config.begin = LogAddress(256);
        let mut replay = Replay::new(config).unwrap();
        let bytes = replay
            .page(&page(1, &[(0, 8969, 0, Some(0), false, false)]), &U64Key)
            .unwrap();
        let frame = PageFrame::decode(&bytes, PageId(1), 256).unwrap();
        assert_eq!(frame.records().unwrap()[0].1.header.previous, None);
        let mut replay = Replay::new(options(256, 2)).unwrap();
        assert!(
            replay
                .page(
                    &page(
                        0,
                        &[
                            (0, 8969, 0, None, false, true),
                            (80, 8969, 0, Some(0), false, false)
                        ]
                    ),
                    &U64Key
                )
                .is_err()
        );
        let bytes = replay
            .page(&page(0, &[(0, 8969, 0, None, false, true)]), &U64Key)
            .unwrap();
        assert!(
            PageFrame::decode(&bytes, PageId(0), 256)
                .unwrap()
                .records()
                .unwrap()
                .is_empty()
        );
        assert!(replay.index().unwrap().entries.is_empty());
        assert!(
            Replay::new(options(0, 0))
                .unwrap()
                .index()
                .unwrap()
                .entries
                .is_empty()
        );
    }
}
