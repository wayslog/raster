//! 显式磁盘编码；v1 已经 P5 恢复协议验收冻结，不兼容变化须显式升级版本。
mod manifest;
mod record;
mod wire;
pub(crate) use manifest::{
    Commit, Kind, MAX_ITEMS as MAX_MANIFEST_ITEMS, Manifest, Material, match_recovery,
};
pub(crate) use record::{HEADER_BYTES, Record, RecordHeader};
mod page;
pub(crate) use page::PageFrame;

pub(crate) use wire::checksum;

mod index;
pub(crate) use index::{IndexEntry, IndexSnapshot};
