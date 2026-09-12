//! Physical record scan is not a valid key snapshot;The first version of the interface returns owned results.
use crate::{
    schema::{OwnedKeyOf, OwnedValueOf, Schema},
    types::*,
};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum Buffering {
    /// Read the current page on demand,Do not pre-read subsequent pages.
    Unbuffered,
    /// Pre-read the next page beyond the current page,Maximum of two pages total.
    SinglePage,
    /// Pre-read the next two pages beyond the current page,Maximum of three pages in total.
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
    /// Canonical decoding of owned keys;invalid Records must also have decodable keys,Otherwise, the scan will report an error.
    pub key: OwnedKeyOf<S>,
    /// Ordinary records return owned values;tombstone or invalid record return None,Value decoding is not called.
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
    /// Return an owned physical record; after completion, repeated calls still return None.
    /// Busy,DeadlineExceeded,OutOfMemory Can be retried;Other errors close scanning,Expert panics as failure shuts down engine.
    pub fn next_record(&mut self) -> Result<Option<ScannedRecord<S>>, Error> {
        self.engine.scan_next(self.registration, &self.state)
    }
    /// Stop read-ahead and wait for accepted reads to return;use Config.scan.timeout,Can be closed again after timeout.
    pub fn close(&mut self) -> Result<(), Error> {
        self.engine.close_scan(self.registration, &self.state)
    }
}
impl<S: Schema> Drop for RecordScanner<S> {
    fn drop(&mut self) {
        self.engine.abandon_scan(self.registration, &self.state);
    }
}
