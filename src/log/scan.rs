//! Single resident record copy;Release all page references before returning,Do not generate globally consistent snapshots.
use super::*;
impl<V: ValueLayout> HybridLog<V> {
    /// For testing only:Simulation P7 The logic of publishing the flushed range begin,No physical deletion is performed.
    #[cfg(test)]
    pub fn advance_begin_for_scan_test(&self, begin: LogAddress) {
        let mut state = self.state.write().unwrap();
        assert!(state.frontiers.begin <= begin && begin <= state.frontiers.head);
        state.frontiers.begin = begin;
    }

    /// Verify logical range and resident slot boundaries before opening scan;Non-aligned boundaries of cold pages are checked separately by the page cursor.
    pub fn validate_scan_range(
        &self,
        begin: LogAddress,
        end: LogAddress,
    ) -> Result<Frontiers, Error> {
        begin.validate()?;
        end.validate()?;
        let state = self
            .state
            .read()
            .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
        let mut frontiers = state.frontiers;
        frontiers.tail = self.pool.tail()?;
        if begin > end || end > frontiers.tail {
            return Err(Error::InvalidFormat("Scan range is invalid"));
        }
        if begin < frontiers.begin {
            return Err(Error::RangeTruncated);
        }
        if begin == end {
            return Ok(frontiers);
        }
        if state.reservations != 0 {
            return Err(Error::Busy);
        }
        let records = self
            .records
            .lock()
            .map_err(|_| Error::InvalidState("Record table lock poisoning"))?;
        for boundary in [begin, end] {
            if boundary >= frontiers.head
                && let Some((address, value)) = records.range(..boundary).next_back()
                && address.checked_add(value.record_bytes() as u64)? > boundary
            {
                return Err(Error::InvalidFormat(
                    "Scan boundary is in the middle of the record",
                ));
            }
        }
        Ok(frontiers)
    }

    /// Only used for current memory range.head forward movement leads to RangeTruncated time,Drivers can reselect disk paths.
    pub fn snapshot_next(
        &self,
        begin: LogAddress,
        end: LogAddress,
    ) -> Result<Option<(LogAddress, Vec<u8>)>, Error> {
        begin.validate()?;
        end.validate()?;
        let selected = {
            let state = self
                .state
                .read()
                .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
            if begin > end || end > self.pool.tail()? {
                return Err(Error::InvalidFormat("Scan range is invalid"));
            }
            if begin < state.frontiers.begin || begin < state.frontiers.head {
                return Err(Error::RangeTruncated);
            }
            if begin == end {
                return Ok(None);
            }
            // Do not cross unpublished slots;Let the caller try again later,Rather than missing late records.
            if state.reservations != 0 {
                return Err(Error::Busy);
            }
            let records = self
                .records
                .lock()
                .map_err(|_| Error::InvalidState("Record table lock poisoning"))?;
            for boundary in [begin, end] {
                if let Some((address, value)) = records.range(..boundary).next_back()
                    && address.checked_add(value.record_bytes() as u64)? > boundary
                {
                    return Err(Error::InvalidFormat(
                        "Scan boundary is in the middle of the record",
                    ));
                }
            }
            records
                .range(begin..end)
                .next()
                .map(|(address, value)| (*address, value.clone()))
        };
        let Some((address, value)) = selected else {
            return Ok(None);
        };
        // Coding Expert lays out without holding bounds or record table locks;Record own permission to exclude concurrent updates.
        let bytes = value.snapshot_record()?;
        drop(value);
        if begin < self.frontiers()?.begin {
            return Err(Error::RangeTruncated);
        }
        Ok(Some((address, bytes)))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{format::Record, schema::builtin::AtomicU64Value};
    fn log() -> HybridLog<AtomicU64Value> {
        HybridLog::new(
            LogConfig {
                page_bytes: 4096,
                memory_pages: 2,
                mutable_fraction: 0.5,
            },
            Arc::new(AtomicU64Value),
        )
        .unwrap()
    }
    #[test]
    fn scanned_copies_are_independent_of_subsequent_in_place_modifications_and_do_not_freeze_records()
     {
        let log = log();
        let address = log
            .finish_initialization(
                log.reserve_record(b"key", None, 7)
                    .unwrap()
                    .with_version(CheckpointVersion(3)),
            )
            .unwrap();
        let before = log.frontiers().unwrap();
        let (_, bytes) = log.snapshot_next(address, before.tail).unwrap().unwrap();
        let record = Record::decode(&bytes).unwrap();
        assert_eq!(record.header.version, CheckpointVersion(3));
        assert_eq!(
            ValueLayout::decode_owned(&AtomicU64Value, record.value).unwrap(),
            7
        );
        let lease = log.lease(address).unwrap();
        lease
            .update(|value| {
                value.store(9, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        lease
            .update(|_| {
                assert!(matches!(
                    log.snapshot_next(address, before.tail),
                    Err(Error::Busy)
                ));
                Ok(())
            })
            .unwrap();
        drop(lease);
        let (_, next) = log.snapshot_next(address, before.tail).unwrap().unwrap();
        assert_eq!(Record::decode(&next).unwrap().value, 9u64.to_le_bytes());
        assert_eq!(log.frontiers().unwrap().read_only, before.read_only);
        drop(log);
        assert_eq!(Record::decode(&bytes).unwrap().value, 7u64.to_le_bytes());
    }
    #[test]
    fn scanning_skips_abandoned_slots_and_rejects_half_record_boundaries_and_unreleased_slots() {
        let log = log();
        let first = log
            .finish_initialization(log.reserve_record(b"a", None, 1).unwrap())
            .unwrap();
        let after_first = log.frontiers().unwrap().tail;
        drop(log.reserve_record("give up".as_bytes(), None, 2).unwrap());
        let next = log
            .finish_initialization(log.reserve_record(b"b", Some(first), 3).unwrap())
            .unwrap();
        let end = log.frontiers().unwrap().tail;
        assert_eq!(
            log.snapshot_next(after_first, end).unwrap().unwrap().0,
            next
        );
        assert!(
            log.snapshot_next(first.checked_add(1).unwrap(), end)
                .is_err()
        );
        assert!(
            log.snapshot_next(first, first.checked_add(1).unwrap())
                .is_err()
        );
        assert!(log.snapshot_next(end, end).unwrap().is_none());
        let pending = log.reserve_record(b"pending", None, 4).unwrap();
        assert!(matches!(log.snapshot_next(first, end), Err(Error::Busy)));
        drop(pending);
        assert_eq!(log.snapshot_next(first, end).unwrap().unwrap().0, first);
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    use crate::schema::builtin::AtomicU64Value;
    #[test]
    fn initialized_undecided_release_slots_maintain_scan_backpressure_and_conflicts_can_be_cleaned_up()
     {
        let log = HybridLog::new(
            LogConfig {
                page_bytes: 4096,
                memory_pages: 2,
                mutable_fraction: 0.5,
            },
            Arc::new(AtomicU64Value),
        )
        .unwrap();
        let reservation = log.reserve_record(&7u64.to_le_bytes(), None, 42).unwrap();
        let end = log.frontiers().unwrap().tail;
        log.with_initialization(reservation, |address| {
            assert!(matches!(
                log.snapshot_next(LogAddress(0), end),
                Err(Error::Busy)
            ));
            assert!(matches!(log.pad_tail(), Err(Error::Busy)));
            assert!(matches!(
                log.advance_read_only(LogAddress(0)),
                Err(Error::Busy)
            ));
            log.retire(address)
        })
        .unwrap();
        assert!(log.snapshot_next(LogAddress(0), end).unwrap().is_none());
        assert!(log.pad_tail().is_ok());
    }
}
