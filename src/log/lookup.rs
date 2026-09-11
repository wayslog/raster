//! 混合日志链查询；等待只保存逻辑地址、规范键编码和拥有型页字节。
use super::{
    read_page::{PageRead, ReadPage},
    *,
};
use crate::{
    device::{CompletionRoute, IoCompletion},
    storage::SegmentedStorage,
};
pub(crate) enum LookupStep<V: ValueLayout> {
    Resident(RecordLease<V>),
    Decoded(value::TemporaryValue<V>),
    Present,
    Tombstone,
    Missing,
    AwaitingIo,
    Continue,
}
pub(crate) struct LogLookup {
    owner: Arc<crate::sync::Mutex<LogState>>,
    storage: Arc<()>,
    key: Vec<u8>,
    next: Option<LogAddress>,
    route: CompletionRoute,
    reading: Option<(PageId, PageRead)>,
    cached: Option<(PageId, ReadPage)>,
    ended: bool,
    matched: Option<LogAddress>,
    needs_value: bool,
}
impl<V: ValueLayout> HybridLog<V> {
    pub fn lookup_metadata(
        &self,
        storage: &SegmentedStorage,
        key: Vec<u8>,
        head: Option<LogAddress>,
        route: CompletionRoute,
    ) -> Result<LogLookup, Error> {
        let mut lookup = self.lookup(storage, key, head, route)?;
        lookup.needs_value = false;
        Ok(lookup)
    }
    /// key 必须来自 KeyCodec 的规范编码，不能传入任意未验证的持久字节。
    pub fn lookup(
        &self,
        storage: &SegmentedStorage,
        key: Vec<u8>,
        head: Option<LogAddress>,
        route: CompletionRoute,
    ) -> Result<LogLookup, Error> {
        if let Some(address) = head {
            address.validate()?;
        }
        Ok(LogLookup {
            owner: self.state.clone(),
            storage: storage.identity.clone(),
            key,
            next: head,
            route,
            reading: None,
            cached: None,
            ended: false,
            matched: None,
            needs_value: true,
        })
    }
}
impl LogLookup {
    pub fn has_inflight(&self) -> bool {
        self.reading
            .as_ref()
            .is_some_and(|(_, reading)| reading.has_inflight())
    }
    pub fn matched_address(&self) -> Option<LogAddress> {
        self.matched.filter(|_| self.ended)
    }
    /// 成功元数据匹配后的同步源快照。驻留源的独占许可一直覆盖闭包，冷源使用已校验的拥有页。
    pub fn with_matched_record<V: ValueLayout, R>(
        &self,
        log: &HybridLog<V>,
        storage: &SegmentedStorage,
        publish: impl FnOnce(&[u8]) -> Result<R, Error>,
    ) -> Result<R, Error> {
        if !Arc::ptr_eq(&self.owner, &log.state) || !Arc::ptr_eq(&self.storage, &storage.identity) {
            return Err(Error::InvalidState("源记录查询归属不匹配"));
        }
        let address = self
            .matched_address()
            .ok_or(Error::InvalidState("查询没有已完成的匹配记录"))?;
        let frontiers = log.frontiers()?;
        if address < frontiers.begin {
            return Err(Error::RangeTruncated);
        }
        if address >= frontiers.head {
            let lease = log.lease(address)?;
            if lease.key() != self.key {
                return Err(Error::InvalidState("源记录键已改变"));
            }
            return lease.value.with_record_snapshot(publish);
        }
        let (_, page) = self.cached.as_ref().ok_or(Error::Busy)?;
        let record = page.record(address)?;
        if record.key != self.key {
            return Err(Error::InvalidFormat("磁盘源记录键不匹配"));
        }
        let length = record.header.encoded_len()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| Error::OutOfMemory)?;
        bytes.resize(length, 0);
        record.encode(&mut bytes)?;
        publish(&bytes)
    }
    /// 只在成功解码冷记录后调用；返回拥有型编码，不暴露页借用。
    pub fn cache_record(&self, limit: usize) -> Result<Option<(LogAddress, Vec<u8>)>, Error> {
        if !self.ended {
            return Ok(None);
        }
        let Some(address) = self.next else {
            return Ok(None);
        };
        let Some((_, page)) = &self.cached else {
            return Ok(None);
        };
        let record = page.record(address)?;
        if record.header.invalid || record.header.tombstone {
            return Ok(None);
        }
        let length = record.header.encoded_len()?;
        if length > limit {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| Error::OutOfMemory)?;
        bytes.resize(length, 0);
        record.encode(&mut bytes)?;
        Ok(Some((address, bytes)))
    }
    #[allow(clippy::result_large_err, reason = "错误路由原样归还完成缓冲")]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        if !Arc::ptr_eq(&self.storage, &storage.identity) {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("查询属于其他存储"),
            });
        }
        let Some((_, reading)) = self.reading.as_mut() else {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("查询没有等待该 I/O"),
            });
        };
        reading.accept(storage, completion)
    }
    pub fn step<V: ValueLayout>(
        &mut self,
        log: &HybridLog<V>,
        storage: &SegmentedStorage,
        budget: PollBudget,
    ) -> Result<LookupStep<V>, Error> {
        if !Arc::ptr_eq(&self.owner, &log.state) || !Arc::ptr_eq(&self.storage, &storage.identity) {
            return Err(Error::InvalidState("查询归属不匹配"));
        }
        if self.ended {
            return Err(Error::InvalidState("查询已经终结"));
        }
        let result = self.advance(log, storage, budget);
        if !matches!(result, Ok(LookupStep::AwaitingIo | LookupStep::Continue)) {
            self.ended = true;
        }
        result
    }
    fn advance<V: ValueLayout>(
        &mut self,
        log: &HybridLog<V>,
        storage: &SegmentedStorage,
        budget: PollBudget,
    ) -> Result<LookupStep<V>, Error> {
        if let Some((page, reading)) = self.reading.as_mut() {
            if let Some(bytes) = reading.finish(storage)? {
                self.cached = Some((*page, bytes));
                self.reading = None;
            } else {
                return match reading.submit_next(storage) {
                    Ok(_) => Ok(LookupStep::AwaitingIo),
                    Err(Error::Busy) => Ok(LookupStep::Continue),
                    Err(error) => Err(error),
                };
            }
        }
        for _ in 0..budget.0.get() {
            let Some(address) = self.next else {
                return Ok(LookupStep::Missing);
            };
            let frontiers = log.frontiers()?;
            if address < frontiers.begin {
                return Ok(LookupStep::Missing);
            }
            if address >= frontiers.tail {
                return Err(Error::InvalidFormat("查询地址超过日志尾部"));
            }
            if address >= frontiers.head {
                let lease = match log.lease(address) {
                    Ok(lease) => lease,
                    Err(Error::RangeTruncated) if address < log.frontiers()?.head => continue,
                    Err(error) => return Err(error),
                };
                if lease.key() == self.key {
                    self.matched = Some(address);
                    return Ok(if lease.is_tombstone() {
                        LookupStep::Tombstone
                    } else if !self.needs_value {
                        LookupStep::Present
                    } else {
                        LookupStep::Resident(lease)
                    });
                }
                self.next = lease.previous();
            } else {
                let page = address.page_offset(log.page_bytes as u64)?.0;
                if let Some((cached_page, bytes)) = &self.cached
                    && *cached_page == page
                {
                    let record = bytes.record(address)?;
                    if record.key == self.key {
                        self.matched = Some(address);
                        return Ok(if record.header.tombstone {
                            LookupStep::Tombstone
                        } else if !self.needs_value {
                            LookupStep::Present
                        } else {
                            LookupStep::Decoded(log.decode_temporary(record.value)?)
                        });
                    }
                    self.next = record.header.previous;
                } else {
                    self.cached = None;
                    let mut reading = PageRead::new(page, log.page_bytes, self.route)?;
                    let submitted = reading.submit_next(storage);
                    self.reading = Some((page, reading));
                    return match submitted {
                        Ok(_) => Ok(LookupStep::AwaitingIo),
                        Err(Error::Busy) => Ok(LookupStep::Continue),
                        Err(error) => Err(error),
                    };
                }
            }
            if self.next.is_some_and(|previous| previous >= address) {
                return Err(Error::InvalidFormat("日志前驱没有严格递减"));
            }
        }
        Ok(LookupStep::Continue)
    }
}
