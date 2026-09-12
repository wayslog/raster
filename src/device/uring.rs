//! Linux io_uring Factory skeleton;feature It does not mean that the kernel driver has been connected.
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
