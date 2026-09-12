//! Eliminate only the memory address table;The page slot is released after the old lease is exited,Do not modify the disk index chain.
use super::*;
impl<V: ValueLayout> HybridLog<V> {
    pub fn evict_next(&self) -> Result<Progress, Error> {
        let retired = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
            let mut retired = Vec::new();
            if state.reclaim.is_none() {
                let begin = state.frontiers.head;
                let end = begin.checked_add(self.page_bytes as u64)?;
                if end > state.frontiers.flushed_until || end > state.frontiers.safe_read_only {
                    return Err(Error::Busy);
                }
                let page = begin.page_offset(self.page_bytes as u64)?.0;
                let generation = self.pool.generation(page)?;
                let mut records = self
                    .records
                    .lock()
                    .map_err(|_| Error::InvalidState("Record table lock poisoning"))?;
                let count = records
                    .range(begin..end)
                    .try_fold(0usize, |count, record| record.map(|_| count + 1))?;
                retired
                    .try_reserve_exact(count)
                    .map_err(|_| Error::OutOfMemory)?;
                // Address table removal and head Publishing is done in the same control phase;No waiting for old lease.
                let mut cursor = begin;
                while let Some(address) = records
                    .range(cursor..end)
                    .next()
                    .transpose()?
                    .map(|(address, _)| address)
                {
                    retired.push(
                        records
                            .remove(&address)?
                            .expect("The address is still in the table"),
                    );
                    cursor = address.checked_add(1)?;
                }
                state.reclaim = Some((page, generation));
                state.frontiers.head = end;
            }
            retired
        };
        // Expert destruction is not performed within the control lock;Destruction failure retains allocation and blocks safe_head move forward.
        drop(retired);
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("Log boundary lock poisoning"))?;
        let Some((page, generation)) = state.reclaim else {
            return Ok(Progress::default());
        };
        match self.pool.release(page, generation) {
            Ok(()) => {
                state.frontiers.safe_head = state.frontiers.head;
                state.reclaim = None;
                Ok(Progress {
                    completed: 1,
                    remaining: 0,
                    phase_advanced: true,
                })
            }
            Err(Error::Busy) => Ok(Progress {
                completed: 0,
                remaining: 1,
                phase_advanced: false,
            }),
            Err(error) => Err(error),
        }
    }
}
