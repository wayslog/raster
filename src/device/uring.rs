//! Linux io_uring 工厂骨架；feature 不代表内核驱动已经接入。
use super::*;

#[derive(Clone, Debug)]
pub struct UringDeviceFactory {
    pub queue_depth: u32,
}
impl DeviceFactory for UringDeviceFactory {
    fn open(&self, _options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Err(Error::unimplemented("device::uring"))
    }
}
