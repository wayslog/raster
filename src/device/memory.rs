//! 可控内存设备的工厂骨架；未来用于完成重排、失败注入和恢复契约测试。
use super::*;

#[derive(Clone, Debug, Default)]
pub struct MemoryDeviceFactory;
impl DeviceFactory for MemoryDeviceFactory {
    fn open(&self, _options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Err(Error::unimplemented("device::memory"))
    }
}
