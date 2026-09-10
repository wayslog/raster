//! 物理记录扫描不是有效键快照；首版接口返回拥有型结果。
use crate::{
    schema::{OwnedKeyOf, OwnedValueOf, Schema},
    types::*,
};
use std::sync::Arc;

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
    pub(crate) engine: Arc<crate::engine::Engine<S>>,
    pub(crate) registration: u64,
    pub(crate) state: Arc<crate::engine::scan::ScanHandle>,
}
impl<S: Schema> RecordScanner<S> {
    pub(crate) fn open(
        engine: Arc<crate::engine::Engine<S>>,
        options: ScanOptions,
    ) -> Result<Self, Error> {
        engine.open_scan(options)
    }
    /// 返回拥有型物理记录；结束后重复调用仍为 None。
    /// Busy、DeadlineExceeded、OutOfMemory 可重试；其他错误关闭扫描，专家恐慌同时失败关闭引擎。
    pub fn next_record(&mut self) -> Result<Option<ScannedRecord<S>>, Error> {
        self.engine.scan_next(self.registration, &self.state)
    }
    /// 停止预读并等待已接受读取归还；使用 Config.scan.timeout，超时后可再次关闭。
    pub fn close(&mut self) -> Result<(), Error> {
        self.engine.close_scan(self.registration, &self.state)
    }
}
impl<S: Schema> Drop for RecordScanner<S> {
    fn drop(&mut self) {
        self.engine.abandon_scan(self.registration, &self.state);
    }
}
