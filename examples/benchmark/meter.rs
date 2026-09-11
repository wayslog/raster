//! 计量设备实际完成字节；接受登记与完成匹配共享锁，不能因提交/轮询竞争漏计。
use raster::{
    device::*,
    types::{Deadline, Error, IoId, PollBudget},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
#[derive(Clone, Copy, Debug, Default)]
pub struct Counts {
    pub read: u64,
    pub written: u64,
}
#[derive(Clone, Default)]
pub struct Meter(Arc<Mutex<Counts>>);
impl Meter {
    pub fn snapshot(&self) -> Counts {
        *self.0.lock().expect("基准计量锁未中毒")
    }
    pub fn wrap(&self, inner: Arc<dyn Device>) -> Metered {
        Metered {
            inner,
            counts: self.clone(),
            ledger: Mutex::new(BTreeMap::new()),
        }
    }
}
impl DeviceFactory for Meter {
    fn open(&self, options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        let inner = thread_pool::ThreadPoolDeviceFactory {
            workers: 2,
            queue_capacity: 128,
        }
        .open(options)?;
        Ok(Box::new(self.wrap(Arc::from(inner))))
    }
}
#[derive(Clone, Copy)]
enum Direction {
    Read(usize),
    Write(usize),
    Other,
}
pub struct Metered {
    inner: Arc<dyn Device>,
    counts: Meter,
    ledger: Mutex<BTreeMap<IoId, Direction>>,
}
impl Device for Metered {
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        let mut ledger = self.ledger.lock().expect("基准路由锁未中毒");
        let direction = match &request.operation {
            IoOperation::Read { buffer, .. } => Direction::Read(buffer.len()),
            IoOperation::Write { buffer, .. } => Direction::Write(buffer.len()),
            _ => Direction::Other,
        };
        let id = self.inner.submit(request)?;
        assert!(
            ledger.insert(id, direction).is_none(),
            "设备不能复用在途标识"
        );
        Ok(id)
    }
    fn poll(&self, budget: PollBudget, output: &mut Vec<IoCompletion>) -> Result<(), Error> {
        let mut ledger = self.ledger.lock().expect("基准路由锁未中毒");
        let begin = output.len();
        let result = self.inner.poll(budget, output);
        let mut counts = self.counts.0.lock().expect("基准计量锁未中毒");
        for completion in &output[begin..] {
            let direction = ledger
                .remove(&completion.id)
                .ok_or(Error::InvalidState("计量设备返回未知完成"))?;
            if let Ok(IoOutcome::Transferred(bytes)) = completion.result {
                let (limit, total) = match direction {
                    Direction::Read(limit) => (limit, &mut counts.read),
                    Direction::Write(limit) => (limit, &mut counts.written),
                    Direction::Other => return Err(Error::InvalidState("元数据不能返回传输字节")),
                };
                if bytes > limit {
                    return Err(Error::InvalidState("传输字节超过请求范围"));
                }
                *total = total
                    .checked_add(bytes as u64)
                    .ok_or(Error::CapacityExceeded)?;
            }
        }
        result
    }
    fn shutdown(&self, deadline: Deadline) -> Result<(), Error> {
        self.inner.shutdown(deadline)
    }
}
