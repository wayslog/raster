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
    version: CheckpointVersion,
    route: CompletionRoute,
    reading: Option<(PageId, PageRead)>,
    cached: Option<(PageId, ReadPage)>,
    ended: bool,
}
impl<V: ValueLayout> HybridLog<V> {
    /// key 必须来自 KeyCodec 的规范编码，不能传入任意未验证的持久字节。
    pub fn lookup(
        &self,
        storage: &SegmentedStorage,
        key: Vec<u8>,
        head: Option<LogAddress>,
        version: CheckpointVersion,
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
            version,
            route,
            reading: None,
            cached: None,
            ended: false,
        })
    }
}
impl LogLookup {
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
                return Err(Error::RangeTruncated);
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
                    return Ok(if lease.is_tombstone() {
                        LookupStep::Tombstone
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
                        return Ok(if record.header.tombstone {
                            LookupStep::Tombstone
                        } else {
                            LookupStep::Decoded(log.decode_temporary(record.value)?)
                        });
                    }
                    self.next = record.header.previous;
                } else {
                    self.cached = None;
                    let mut reading =
                        PageRead::new(page, log.page_bytes, self.version, self.route)?;
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
