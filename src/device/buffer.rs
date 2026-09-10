//! 安全的零初始化对齐缓冲；通过额外分配调整切片起点，当前不使用裸分配器。

use crate::types::Error;

pub struct AlignedBuffer {
    storage: Vec<u8>,
    start: usize,
    length: usize,
    alignment: usize,
}
impl AlignedBuffer {
    pub fn new_zeroed(length: usize, alignment: usize) -> Result<Self, Error> {
        if length == 0 || !alignment.is_power_of_two() {
            return Err(Error::InvalidConfig {
                field: "buffer",
                reason: "长度须非零，对齐须为二次幂",
            });
        }
        let total = length
            .checked_add(alignment - 1)
            .ok_or(Error::CapacityExceeded)?;
        let mut storage = Vec::new();
        storage
            .try_reserve_exact(total)
            .map_err(|_| Error::OutOfMemory)?;
        storage.resize(total, 0);
        let address = storage.as_ptr() as usize;
        let start = (alignment - address % alignment) % alignment;
        Ok(Self {
            storage,
            start,
            length,
            alignment,
        })
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.storage[self.start..self.start + self.length]
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.storage[self.start..self.start + self.length]
    }
    pub fn alignment(&self) -> usize {
        self.alignment
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}
impl std::fmt::Debug for AlignedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuffer")
            .field("length", &self.length)
            .field("alignment", &self.alignment)
            .finish_non_exhaustive()
    }
}
