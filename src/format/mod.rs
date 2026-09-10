//! 显式磁盘编码；开发期 v1 在 P5 恢复协议验收后冻结。
mod manifest;
mod record;
mod wire;
pub(crate) use manifest::Manifest;
