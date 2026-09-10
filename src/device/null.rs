//! 无后备存储设备的工厂骨架；不能将其作为可恢复的内存磁盘。
use super::*;

#[derive(Clone, Debug, Default)]
pub struct NullDeviceFactory;
impl DeviceFactory for NullDeviceFactory {
    fn open(&self, _options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Err(Error::unimplemented("device::null"))
    }
}
