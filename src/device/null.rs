//! Devices without backing storage:Can run restricted memory instances,reject all I/O,No commitment to restore.
use super::*;
#[derive(Clone, Debug, Default)]
pub struct NullDeviceFactory;
struct NullDevice;
impl DeviceFactory for NullDeviceFactory {
    fn open(&self, _options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        Ok(Box::new(NullDevice))
    }
}
impl Device for NullDevice {
    fn capabilities(&self) -> DeviceCapabilities {
        DeviceCapabilities {
            supports_files: false,
            memory_alignment: 1,
            transfer_alignment: 1,
            supports_file_sync: false,
            supports_directory_sync: false,
            supports_atomic_publish: false,
            supports_directory_listing: false,
            supports_file_locks: false,
        }
    }
    fn submit(&self, request: IoRequest) -> Result<IoId, RejectedIo> {
        Err(RejectedIo {
            request,
            reason: Error::UnsupportedDurability,
        })
    }
    fn poll(&self, _budget: PollBudget, _output: &mut Vec<IoCompletion>) -> Result<(), Error> {
        Ok(())
    }
    fn shutdown(&self, _deadline: Deadline) -> Result<(), Error> {
        Ok(())
    }
}
