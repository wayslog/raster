//! 物理记录扫描不是有效键快照；首版接口返回拥有型结果。
use crate::{
    schema::{OwnedKeyOf, OwnedValueOf, Schema},
    types::*,
};
use std::marker::PhantomData;

#[derive(Clone, Copy, Debug)]
pub enum Buffering {
    /// 按需读取当前页，不预读后续页。
    Unbuffered,
    /// 当前页之外预读后续一页，总计最多两页。
    SinglePage,
    /// 当前页之外预读后续两页，总计最多三页。
    DoublePage,
}
#[derive(Clone, Copy, Debug)]
pub struct ScanOptions {
    pub begin: LogAddress,
    pub end: LogAddress,
    pub buffering: Buffering,
}
pub struct ScannedRecord<S: Schema> {
    pub address: LogAddress,
    pub version: CheckpointVersion,
    /// 规范解码的拥有型键；invalid 记录也必须具有可解码键，否则扫描报错。
    pub key: OwnedKeyOf<S>,
    /// 普通记录返回拥有型值；墓碑或 invalid 记录返回 None，不调用值解码。
    pub value: Option<OwnedValueOf<S>>,
    pub tombstone: bool,
    pub invalid: bool,
}
pub struct RecordScanner<S: Schema> {
    pub(crate) schema: PhantomData<S>,
}
impl<S: Schema> RecordScanner<S> {
    pub fn next_record(&mut self) -> Result<Option<ScannedRecord<S>>, Error> {
        Err(Error::unimplemented("scan::next_record"))
    }
    pub fn close(&mut self) -> Result<(), Error> {
        Err(Error::unimplemented("scan::close"))
    }
}
