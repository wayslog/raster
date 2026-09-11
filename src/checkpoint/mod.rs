//! 检查点材料、提交证据和恢复重放；不会把启动或文件写完当作持久化成功。
pub(crate) mod catalog;
pub(crate) mod catalog_lock;
pub(crate) mod directory;
pub(crate) mod log_material;
pub(crate) mod manifest_read;
pub(crate) mod material;
pub(crate) mod publication;
pub(crate) mod read;

pub(crate) mod recovery;
pub(crate) mod replay;

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod publication_tests;

pub(crate) mod retention;
