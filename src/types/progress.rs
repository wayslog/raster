//! Promotion budget and deadline do not include persistence success implications.

use std::num::NonZeroUsize;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub struct Deadline(pub Instant);
impl Deadline {
    pub fn expired(self) -> bool {
        Instant::now() >= self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PollBudget(pub NonZeroUsize);
impl Default for PollBudget {
    fn default() -> Self {
        Self(NonZeroUsize::new(64).expect("Fixed budget is non-zero"))
    }
}
#[derive(Clone, Copy, Debug)]
pub enum WaitMode {
    Once,
    Until(Deadline),
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    pub completed: usize,
    pub remaining: usize,
    pub phase_advanced: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainReport {
    Pending(Progress),
    Drained,
}
