//! explicit disk encoding;v1 Already P5 Recovery Agreement Acceptance Freeze,Incompatible changes require an explicit upgrade version.
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
