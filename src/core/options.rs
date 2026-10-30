//! Optional operation behavior, specialized away from default entry points.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadOptions {
    /// A matching tombstone completes with Aborted instead of NotFound.
    pub abort_if_tombstone: bool,
}
#[derive(Clone, Copy, Debug)]
pub struct RmwOptions {
    /// When false, a missing or deleted key completes with NotFound.
    pub create_if_not_exists: bool,
}
impl Default for RmwOptions {
    fn default() -> Self {
        Self {
            create_if_not_exists: true,
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct DeleteOptions {
    /// Append a tombstone even when the key has never existed.
    pub force_tombstone: bool,
}
