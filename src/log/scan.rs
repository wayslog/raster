//! 单条驻留记录复制；返回前释放所有页引用，不生成全局一致快照。
use super::*;
impl<V: ValueLayout> HybridLog<V> {
    /// 测试专用：模拟 P7 发布已刷盘范围的逻辑 begin，不执行物理删除。
    #[cfg(test)]
    pub fn advance_begin_for_scan_test(&self, begin: LogAddress) {
        let mut state = self.state.lock().unwrap();
        assert!(state.frontiers.begin <= begin && begin <= state.frontiers.head);
        state.frontiers.begin = begin;
    }

    /// 开放扫描前验证逻辑范围和驻留槽边界；冷页的非对齐边界由页游标另行检查。
    pub fn validate_scan_range(
        &self,
        begin: LogAddress,
        end: LogAddress,
    ) -> Result<Frontiers, Error> {
        begin.validate()?;
        end.validate()?;
        let state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
        let mut frontiers = state.frontiers;
        frontiers.tail = self.pool.tail()?;
        if begin > end || end > frontiers.tail {
            return Err(Error::InvalidFormat("扫描范围无效"));
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
            .map_err(|_| Error::InvalidState("记录表锁中毒"))?;
        for boundary in [begin, end] {
            if boundary >= frontiers.head
                && let Some((address, value)) = records.range(..boundary).next_back()
                && address.checked_add(value.record_bytes() as u64)? > boundary
            {
                return Err(Error::InvalidFormat("扫描边界位于记录中间"));
            }
        }
        Ok(frontiers)
    }

    /// 只用于当前内存范围。head 前移导致 RangeTruncated 时，驱动者可重新选择磁盘路径。
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
                .lock()
                .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
            if begin > end || end > self.pool.tail()? {
                return Err(Error::InvalidFormat("扫描范围无效"));
            }
            if begin < state.frontiers.begin || begin < state.frontiers.head {
                return Err(Error::RangeTruncated);
            }
            if begin == end {
                return Ok(None);
            }
            // 不跨过尚未发布的槽；让调用者稍后重试，而不是遗漏迟到记录。
            if state.reservations != 0 {
                return Err(Error::Busy);
            }
            let records = self
                .records
                .lock()
                .map_err(|_| Error::InvalidState("记录表锁中毒"))?;
            for boundary in [begin, end] {
                if let Some((address, value)) = records.range(..boundary).next_back()
                    && address.checked_add(value.record_bytes() as u64)? > boundary
                {
                    return Err(Error::InvalidFormat("扫描边界位于记录中间"));
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
        // 编码专家布局时不持有边界或记录表锁；记录自身许可排除并发更新。
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
    fn 扫描副本独立于后续原地修改且不冻结记录() {
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
    fn 扫描跳过放弃槽且拒绝半记录边界和未发布槽() {
        let log = log();
        let first = log
            .finish_initialization(log.reserve_record(b"a", None, 1).unwrap())
            .unwrap();
        let after_first = log.frontiers().unwrap().tail;
        drop(log.reserve_record("放弃".as_bytes(), None, 2).unwrap());
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
