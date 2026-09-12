//! Windows Native device factory skeleton;OVERLAPPED,IOCP and the handle life cycle has not yet been accessed.
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
