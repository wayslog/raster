//! Mixed log chain query;Wait to save only the logical address,Canonical key encoding and owning page bytes.
use super::{
    read_page::{PageRead, ReadPage},
    *,
};
use crate::{
    device::{CompletionRoute, IoCompletion},
    schema::encoded_key::EncodedKey,
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
    owner: Arc<LogControl>,
    storage: Arc<()>,
    key: EncodedKey,
    next: Option<LogAddress>,
    route: CompletionRoute,
    reading: Option<(PageId, PageRead)>,
    cached: Option<(PageId, ReadPage)>,
    ended: bool,
    matched: Option<LogAddress>,
    needs_value: bool,
    io_enabled: bool,
    awaiting_route: bool,
}
impl<V: ValueLayout> HybridLog<V> {
    pub fn lookup_metadata(
        &self,
        storage: &SegmentedStorage,
        key: impl Into<EncodedKey>,
        head: Option<LogAddress>,
        route: CompletionRoute,
    ) -> Result<LogLookup, Error> {
        let mut lookup = self.lookup(storage, key, head, route)?;
        lookup.needs_value = false;
        Ok(lookup)
    }
    /// key must come from KeyCodec standard encoding,Arbitrary unvalidated persistent bytes cannot be passed in.
    pub fn lookup(
        &self,
        storage: &SegmentedStorage,
        key: impl Into<EncodedKey>,
        head: Option<LogAddress>,
        route: CompletionRoute,
    ) -> Result<LogLookup, Error> {
        if let Some(address) = head {
            address.validate()?;
        }
        Ok(LogLookup {
            owner: self.state.clone(),
            storage: storage.identity.clone(),
            key: key.into(),
            next: head,
            route,
            reading: None,
            cached: None,
            ended: false,
            matched: None,
            needs_value: true,
            io_enabled: true,
            awaiting_route: false,
        })
    }
    pub fn lookup_deferred(
        &self,
        storage: &SegmentedStorage,
        key: impl Into<EncodedKey>,
        head: Option<LogAddress>,
        route: CompletionRoute,
    ) -> Result<LogLookup, Error> {
        let mut lookup = self.lookup(storage, key, head, route)?;
        lookup.io_enabled = false;
        Ok(lookup)
    }
}
impl LogLookup {
    pub fn awaiting_route(&self) -> bool {
        self.awaiting_route
    }
    /// Called only after the owning operation has registered its completion route.
    pub fn enable_io(&mut self) {
        self.io_enabled = true;
        self.awaiting_route = false;
    }
    pub fn has_inflight(&self) -> bool {
        self.reading
            .as_ref()
            .is_some_and(|(_, reading)| reading.has_inflight())
    }
    pub fn matched_address(&self) -> Option<LogAddress> {
        self.matched.filter(|_| self.ended)
    }
    /// Sync source snapshot after successful metadata match.The exclusive permission of the resident source always covers the closure,Cold source uses verified owning page.
    pub fn with_matched_record<V: ValueLayout, R>(
        &self,
        log: &HybridLog<V>,
        storage: &SegmentedStorage,
        publish: impl FnOnce(&[u8]) -> Result<R, Error>,
    ) -> Result<R, Error> {
        if !Arc::ptr_eq(&self.owner, &log.state) || !Arc::ptr_eq(&self.storage, &storage.identity) {
            return Err(Error::InvalidState(
                "Source record query attribution does not match",
            ));
        }
        let address = self.matched_address().ok_or(Error::InvalidState(
            "Query has no completed matching records",
        ))?;
        let frontiers = log.frontiers()?;
        if address < frontiers.begin {
            return Err(Error::RangeTruncated);
        }
        if address >= frontiers.head {
            let lease = log.lease(address)?;
            if lease.key() != &*self.key {
                return Err(Error::InvalidState("Source record key changed"));
            }
            return lease.value.with_record_snapshot(publish);
        }
        let (_, page) = self.cached.as_ref().ok_or(Error::Busy)?;
        let record = page.record(address)?;
        if record.key != &*self.key {
            return Err(Error::InvalidFormat("Disk source record key mismatch"));
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
    /// Only called after successfully decoding a cold record;Returns the owning code,Do not expose page borrowing.
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
    #[allow(
        clippy::result_large_err,
        reason = "The error route is returned intact to the completion buffer."
    )]
    pub fn accept(
        &mut self,
        storage: &SegmentedStorage,
        completion: IoCompletion,
    ) -> Result<(), Rejected<IoCompletion>> {
        if !Arc::ptr_eq(&self.storage, &storage.identity) {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("Query belongs to other storage"),
            });
        }
        let Some((_, reading)) = self.reading.as_mut() else {
            return Err(Rejected {
                request: completion,
                reason: Error::InvalidState("The query is not waiting for the I/O"),
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
            return Err(Error::InvalidState("Query attribution does not match"));
        }
        if self.ended {
            return Err(Error::InvalidState("Query has been terminated"));
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
                return Err(Error::InvalidFormat(
                    "The query address exceeds the end of the log",
                ));
            }
            if address >= frontiers.head {
                let lease = match log.lease(address) {
                    Ok(lease) => lease,
                    Err(Error::RangeTruncated) if address < log.frontiers()?.head => continue,
                    Err(error) => return Err(error),
                };
                if lease.key() == &*self.key {
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
                    if record.key == &*self.key {
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
                    if !self.io_enabled {
                        self.awaiting_route = true;
                        return Ok(LookupStep::Continue);
                    }
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
                return Err(Error::InvalidFormat(
                    "Log predecessor is not strictly decreasing",
                ));
            }
        }
        Ok(LookupStep::Continue)
    }
}
