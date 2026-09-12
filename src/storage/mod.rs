//! segment address,Generation binding and checkpoint naming;actual I/O Still request execution via device.
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
    protection: Arc<()>,
    generation: Generation,
    file: Option<FileId>,
}
/// A short-lived physical read lease. The corresponding segment mapping cannot be
/// removed while it is held; Drop must not acquire the storage lock.
pub(crate) struct SegmentReadLease {
    _bindings: Vec<Arc<()>>,
}
pub(crate) struct SegmentedStorage {
    pub identity: Arc<crate::sync::InstanceId>,
    pub device: Arc<dyn Device>,
    pub segment_bytes: u64,
    segments: Mutex<BTreeMap<u64, Binding>>,
    segment_directory: PathBuf,
}
impl SegmentedStorage {
    pub fn new(device: Arc<dyn Device>, segment_bytes: u64) -> Result<Self, Error> {
        if !segment_bytes.is_power_of_two() {
            return Err(Error::InvalidConfig {
                field: "storage.segment_bytes",
                reason: "Segment size must be a non-zero power of two",
            });
        }
        Ok(Self {
            identity: Arc::new(crate::sync::InstanceId::new()?),
            device,
            segment_bytes,
            segments: Mutex::new(BTreeMap::new()),
            segment_directory: PathBuf::from("segments"),
        })
    }
    /// Use separate directory for recovery output,Cannot overwrite old work logs or checkpoint material.
    pub fn recovered(
        device: Arc<dyn Device>,
        segment_bytes: u64,
        nonce: CheckpointToken,
    ) -> Result<Self, Error> {
        nonce.validate()?;
        let mut storage = Self::new(device, segment_bytes)?;
        let name: String = nonce.0.iter().map(|b| format!("{b:02x}")).collect();
        storage.segment_directory = PathBuf::from(format!("restore-{name}"));
        Ok(storage)
    }
    pub fn segment_directory(&self) -> PathBuf {
        self.segment_directory.clone()
    }
    pub fn bound_files(&self) -> Result<Vec<FileId>, Error> {
        Ok(self
            .segments
            .lock()
            .map_err(|_| Error::InvalidState("segment_map_lock_poisoned"))?
            .values()
            .filter_map(|binding| binding.file)
            .collect())
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
    pub fn generation(&self, number: u64) -> Result<Generation, Error> {
        let segments = self
            .segments
            .lock()
            .map_err(|_| Error::InvalidState("segment_map_lock_poisoned"))?;
        Ok(segments
            .get(&number)
            .map_or(Generation(0), |binding| binding.generation))
    }
    pub fn segment_path(&self, number: u64, generation: Generation) -> PathBuf {
        self.segment_directory
            .join(format!("{number:016x}-{:016x}.log", generation.0))
    }
    pub fn bind(&self, number: u64, generation: Generation, file: FileId) -> Result<(), Error> {
        let mut segments = self
            .segments
            .lock()
            .map_err(|_| Error::InvalidState("segment_map_lock_poisoned"))?;
        match segments.get_mut(&number) {
            Some(binding) if binding.generation == generation && binding.file.is_none() => {
                binding.file = Some(file)
            }
            Some(_) => {
                return Err(Error::InvalidState(
                    "The segment generation has expired or has been bound",
                ));
            }
            None => {
                segments.insert(
                    number,
                    Binding {
                        protection: Arc::new(()),
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
            .map_err(|_| Error::InvalidState("segment_map_lock_poisoned"))?;
        let binding = segments
            .get(&(address.0 / self.segment_bytes))
            .ok_or(Error::RangeTruncated)?;
        Ok(SegmentLocation {
            file: binding.file.ok_or(Error::RangeTruncated)?,
            offset: address.0 % self.segment_bytes,
            generation: binding.generation,
        })
    }
    /// Protect the entire physical read range within the same mapping lock,and invalidate Atomic mutual exclusion.
    pub fn lease_read(&self, start: u64, length: usize) -> Result<SegmentReadLease, Error> {
        let slices = self.split(LogAddress(start), length as u64)?;
        let mut bindings = Vec::new();
        bindings
            .try_reserve_exact(slices.len())
            .map_err(|_| Error::OutOfMemory)?;
        let segments = self
            .segments
            .lock()
            .map_err(|_| Error::InvalidState("segment_map_lock_poisoned"))?;
        for slice in slices {
            let binding = segments.get(&slice.number).ok_or(Error::RangeTruncated)?;
            if binding.file.is_none() {
                return Err(Error::RangeTruncated);
            }
            bindings.push(binding.protection.clone());
        }
        Ok(SegmentReadLease {
            _bindings: bindings,
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
    /// Invalidate the old mapping first,Close the old handle again;New generations must use different physical filenames.
    pub fn invalidate(
        &self,
        number: u64,
        generation: Generation,
    ) -> Result<(FileId, Generation), Error> {
        let mut segments = self
            .segments
            .lock()
            .map_err(|_| Error::InvalidState("segment_map_lock_poisoned"))?;
        let binding = segments.get_mut(&number).ok_or(Error::RangeTruncated)?;
        if binding.generation != generation {
            return Err(Error::RangeTruncated);
        }
        if Arc::strong_count(&binding.protection) != 1 {
            return Err(Error::Busy);
        }
        let next = Generation(generation.0.checked_add(1).ok_or(Error::CapacityExceeded)?);
        let file = binding.file.take().ok_or(Error::RangeTruncated)?;
        binding.generation = next;
        Ok((file, next))
    }
    pub fn checkpoint_material_name(id: u64, generation: Generation) -> String {
        format!("{id:016x}-{:016x}.material", generation.0)
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
            return Err(Error::InvalidFormat("Checkpoint material name is invalid"));
        }
        let token = token
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        Ok(PathBuf::from("checkpoints").join(token).join(object))
    }
    /// The caller must have synchronized the new directory's parent directory entries,and write,sync material,manifest and commit.pending.
    /// This function only constructs the final rename and directory synchronization operations;It is not possible to declare a successful release based on this.
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
        let directory = destination
            .parent()
            .expect("fixed directory hierarchy")
            .to_path_buf();
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
            16,
        )
        .unwrap()
    }
    #[test]
    fn cross_segment_splitting_boundaries_and_overflow_rejection() {
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
    fn new_segment_binding_rejects_late_completion_and_old_generation_reopening() {
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
    fn checkpoint_material_name_and_equipment_capability_check() {
        let s = storage();
        let token = CheckpointToken([1; 16]);
        for name in ["../escape", "/root", "..", "a/b", ""] {
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

pub(crate) mod open;
pub(crate) mod transfer;

pub(crate) mod reclaim;
