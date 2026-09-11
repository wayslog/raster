//! 目录结果按名称和条目双重预算计费；超限不得输出可能误判依赖的部分目录。
use super::*;
pub(super) struct DirectorySnapshot {
    entries: Vec<DirectoryEntry>,
    names: usize,
    max_entries: usize,
    max_names: usize,
}
impl DirectorySnapshot {
    pub fn new(max_entries: usize, max_names: usize) -> Self {
        Self {
            entries: Vec::new(),
            names: 0,
            max_entries,
            max_names,
        }
    }
    pub fn push(&mut self, entry: DirectoryEntry) -> Result<(), Error> {
        let names = self
            .names
            .checked_add(entry.name.as_encoded_bytes().len())
            .ok_or(Error::CapacityExceeded)?;
        if self.entries.len() >= self.max_entries || names > self.max_names {
            return Err(Error::CapacityExceeded);
        }
        self.entries
            .try_reserve(1)
            .map_err(|_| Error::OutOfMemory)?;
        self.entries.push(entry);
        self.names = names;
        Ok(())
    }
    pub fn finish(mut self) -> IoOutcome {
        self.entries
            .sort_unstable_by(|left, right| left.name.cmp(&right.name));
        IoOutcome::Directory(self.entries)
    }
}
