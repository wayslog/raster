//! Windows 原生设备工厂骨架；OVERLAPPED、IOCP 和句柄生命周期尚未接入。
use super::*;

#[derive(Clone, Debug)]
pub struct WindowsDeviceFactory {
    pub queue_capacity: usize,
}
impl DeviceFactory for WindowsDeviceFactory {
    fn open(&self, _options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Err(Error::unimplemented("device::windows"))
    }
}
