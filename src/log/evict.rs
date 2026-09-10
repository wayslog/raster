//! 淘汰只摘除内存地址表；旧租约退出后才释放页槽，不修改磁盘索引链。
use super::*;
impl<V: ValueLayout> HybridLog<V> {
    pub fn evict_next(&self) -> Result<Progress, Error> {
        let retired = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
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
                    .map_err(|_| Error::InvalidState("记录表锁中毒"))?;
                let count = records.range(begin..end).count();
                retired
                    .try_reserve_exact(count)
                    .map_err(|_| Error::OutOfMemory)?;
                // 地址表移除与 head 发布在同一控制阶段完成；不等待旧租约。
                while let Some(address) = records
                    .range(begin..end)
                    .next()
                    .map(|(address, _)| *address)
                {
                    retired.push(records.remove(&address).expect("地址仍在表内"));
                }
                state.reclaim = Some((page, generation));
                state.frontiers.head = end;
            }
            retired
        };
        // 专家析构不在控制锁内执行；析构失败保留分配并阻止 safe_head 前进。
        drop(retired);
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::InvalidState("日志边界锁中毒"))?;
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
