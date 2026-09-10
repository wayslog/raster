//! 分段地址、代次绑定与检查点命名；实际 I/O 仍通过设备请求执行。
use crate::{
    device::{Device, FileId, IoOperation},
    types::*,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};
#[derive(Clone, Copy, Debug)]
pub(crate) struct SegmentLocation {
    pub file: FileId,
    pub offset: u64,
    pub generation: Generation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SegmentSlice {
    pub number: u64,
    pub offset: u64,
    pub length: u64,
}
struct Binding {
    generation: Generation,
    file: Option<FileId>,
}
pub(crate) struct SegmentedStorage {
    pub device: Arc<dyn Device>,
    pub root: PathBuf,
    pub segment_bytes: u64,
    segments: Mutex<BTreeMap<u64, Binding>>,
}
impl SegmentedStorage {
    pub fn new(device: Arc<dyn Device>, root: PathBuf, segment_bytes: u64) -> Result<Self, Error> {
        if !segment_bytes.is_power_of_two() {
            return Err(Error::InvalidConfig {
                field: "storage.segment_bytes",
                reason: "段大小必须是非零二次幂",
            });
        }
        Ok(Self {
            device,
            root,
            segment_bytes,
            segments: Mutex::new(BTreeMap::new()),
        })
    }
    pub fn split(&self, address: LogAddress, length: u64) -> Result<Vec<SegmentSlice>, Error> {
        address.checked_add(length)?;
        let mut at = address.0;
        let mut left = length;
        let mut slices = Vec::new();
        while left > 0 {
            let number = at / self.segment_bytes;
            let offset = at % self.segment_bytes;
            let len = left.min(self.segment_bytes - offset);
            slices.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
            slices.push(SegmentSlice {
                number,
                offset,
                length: len,
            });
            at += len;
            left -= len;
        }
        Ok(slices)
    }
    pub fn segment_path(&self, number: u64, generation: Generation) -> PathBuf {
        PathBuf::from("segments").join(format!("{number:016x}-{:016x}.log", generation.0))
    }
    pub fn bind(&self, number: u64, generation: Generation, file: FileId) -> Result<(), Error> {
        let mut segments = self
            .segments
            .lock()
            .map_err(|_| Error::InvalidState("段映射锁中毒"))?;
        match segments.get_mut(&number) {
            Some(binding) if binding.generation == generation && binding.file.is_none() => {
                binding.file = Some(file)
            }
            Some(_) => return Err(Error::InvalidState("段代次过期或已经绑定")),
            None => {
                segments.insert(
                    number,
                    Binding {
                        generation,
                        file: Some(file),
                    },
                );
            }
        }
        Ok(())
    }
    pub fn resolve(&self, address: LogAddress) -> Result<SegmentLocation, Error> {
        address.validate()?;
        let segments = self
            .segments
            .lock()
            .map_err(|_| Error::InvalidState("段映射锁中毒"))?;
        let binding = segments
            .get(&(address.0 / self.segment_bytes))
            .ok_or(Error::RangeTruncated)?;
        Ok(SegmentLocation {
            file: binding.file.ok_or(Error::RangeTruncated)?,
            offset: address.0 % self.segment_bytes,
            generation: binding.generation,
        })
    }
    pub fn validate_completion(
        &self,
        address: LogAddress,
        location: SegmentLocation,
    ) -> Result<(), Error> {
        let current = self.resolve(address)?;
        if current.file != location.file
            || current.generation != location.generation
            || current.offset != location.offset
        {
            return Err(Error::RangeTruncated);
        }
        Ok(())
    }
    /// 先使旧映射失效，再关闭旧句柄；新一代必须使用不同的物理文件名。
    pub fn invalidate(
        &self,
        number: u64,
        generation: Generation,
    ) -> Result<(FileId, Generation), Error> {
        let mut segments = self
            .segments
            .lock()
            .map_err(|_| Error::InvalidState("段映射锁中毒"))?;
        let binding = segments.get_mut(&number).ok_or(Error::RangeTruncated)?;
        if binding.generation != generation {
            return Err(Error::RangeTruncated);
        }
        let next = Generation(generation.0.checked_add(1).ok_or(Error::CapacityExceeded)?);
        let file = binding.file.take().ok_or(Error::RangeTruncated)?;
        binding.generation = next;
        Ok((file, next))
    }
    pub fn checkpoint_path(&self, token: CheckpointToken, object: &str) -> Result<PathBuf, Error> {
        token.validate()?;
        if object.is_empty()
            || object.len() > 128
            || object == "."
            || object == ".."
            || !object
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(Error::InvalidFormat("检查点材料名无效"));
        }
        let token = token
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        Ok(PathBuf::from("checkpoints").join(token).join(object))
    }
    /// 调用者必须已同步新目录的父目录项，并写入、同步材料、manifest 和 commit.pending。
    /// 本函数只构造最终重命名与目录同步操作；不能据此声明发布成功。
    pub fn publish_plan(&self, token: CheckpointToken) -> Result<Vec<IoOperation>, Error> {
        let caps = self.device.capabilities();
        if !caps.supports_file_sync
            || !caps.supports_directory_sync
            || !caps.supports_atomic_publish
        {
            return Err(Error::UnsupportedDurability);
        }
        let source = self.checkpoint_path(token, "commit.pending")?;
        let destination = self.checkpoint_path(token, "commit")?;
        let directory = destination.parent().expect("固定目录层级").to_path_buf();
        Ok(vec![
            IoOperation::Rename {
                source,
                destination,
            },
            IoOperation::SyncDirectory(directory),
        ])
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn storage() -> SegmentedStorage {
        SegmentedStorage::new(
            Arc::new(crate::device::memory::MemoryDevice::new(8, 128).unwrap()),
            PathBuf::new(),
            16,
        )
        .unwrap()
    }
    #[test]
    fn 跨段切分边界和溢出拒绝() {
        let s = storage();
        assert_eq!(
            s.split(LogAddress(15), 18).unwrap(),
            vec![
                SegmentSlice {
                    number: 0,
                    offset: 15,
                    length: 1
                },
                SegmentSlice {
                    number: 1,
                    offset: 0,
                    length: 16
                },
                SegmentSlice {
                    number: 2,
                    offset: 0,
                    length: 1
                }
            ]
        );
        assert!(s.split(LogAddress(0), 0).unwrap().is_empty());
        assert!(s.split(LogAddress(u64::MAX - 1), 2).is_err());
    }
    #[test]
    fn 新段绑定拒绝迟到完成和旧代次重新打开() {
        let s = storage();
        let old = FileId {
            slot: 0,
            generation: Generation(0),
        };
        s.bind(0, Generation(0), old).unwrap();
        let location = s.resolve(LogAddress(7)).unwrap();
        let (_, generation) = s.invalidate(0, Generation(0)).unwrap();
        assert!(s.validate_completion(LogAddress(7), location).is_err());
        assert!(s.bind(0, Generation(0), old).is_err());
        s.bind(
            0,
            generation,
            FileId {
                slot: 1,
                generation: Generation(0),
            },
        )
        .unwrap();
        assert!(s.validate_completion(LogAddress(7), location).is_err());
        assert_ne!(
            s.segment_path(0, generation),
            s.segment_path(0, Generation(0))
        );
    }
    #[test]
    fn 检查点材料名和设备能力检查() {
        let s = storage();
        let token = CheckpointToken([1; 16]);
        for name in ["../逃逸", "/根", "..", "a/b", ""] {
            assert!(s.checkpoint_path(token, name).is_err());
        }
        assert!(
            s.checkpoint_path(CheckpointToken([0; 16]), "manifest")
                .is_err()
        );
        assert!(s.checkpoint_path(token, "manifest").unwrap().is_relative());
        assert!(matches!(
            s.publish_plan(token),
            Err(Error::UnsupportedDurability)
        ));
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[path = "tests.rs"]
mod native_tests;

pub(crate) mod write;
