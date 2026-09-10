//! 本地文件工作线程后端的配置骨架；尚未启动线程或打开文件。
use super::*;

#[derive(Clone, Debug)]
pub struct ThreadPoolDeviceFactory {
    pub workers: usize,
    pub queue_capacity: usize,
}
impl DeviceFactory for ThreadPoolDeviceFactory {
    fn open(&self, _options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Err(Error::unimplemented("device::thread_pool"))
    }
}
