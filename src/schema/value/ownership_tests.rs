use super::*;
use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    schema::builtin::{ByteValueCodec, SchemaPair, SerializedValue, U64Key},
    types::*,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Bytes {
    value: Vec<u8>,
    clones: Arc<AtomicUsize>,
}
impl Clone for Bytes {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            value: self.value.clone(),
            clones: self.clones.clone(),
        }
    }
}
struct Codec(Arc<AtomicUsize>);
impl ValueCodec for Codec {
    type Value = Bytes;
    fn format_id(&self) -> FormatId {
        ByteValueCodec.format_id()
    }
    fn encode(&self, value: &Bytes) -> Result<Vec<u8>, Error> {
        ByteValueCodec.encode(&value.value)
    }
    fn decode(&self, bytes: &[u8]) -> Result<Bytes, Error> {
        Ok(Bytes {
            value: ByteValueCodec.decode(bytes)?,
            clones: self.0.clone(),
        })
    }
}
type S = SchemaPair<U64Key, SerializedValue<Codec>>;

// Older engines expose only a borrowed accessor and require this extra clone.
#[allow(
    dead_code,
    reason = "The inherent ownership transfer supersedes this baseline fallback."
)]
trait LegacyView {
    fn into_view(self) -> Bytes;
}
impl LegacyView for ValueRead<'_, S> {
    fn into_view(self) -> Bytes {
        self.view().clone()
    }
}
struct Request(Vec<u8>, Arc<AtomicUsize>);
impl Keyed<S> for Request {
    fn key(&self) -> &u64 {
        &1
    }
}
impl ReadOperation<S> for Request {
    type Output = Vec<u8>;
    fn read(&mut self, view: ValueRead<'_, S>) -> Result<Vec<u8>, Error> {
        Ok(view.into_view().value)
    }
}
impl UpsertOperation<S> for Request {
    type Output = ();
    fn update_in_place(&mut self, _: ValueUpdate<'_, S>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
    fn replacement(&mut self) -> Result<(Bytes, ()), Error> {
        Ok((
            Bytes {
                value: self.0.clone(),
                clones: self.1.clone(),
            },
            (),
        ))
    }
}
impl RmwOperation<S> for Request {
    type Output = ();
    fn initial(&mut self) -> Result<(Bytes, ()), Error> {
        self.replacement()
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, S>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
    fn copy_update(&mut self, view: ValueRead<'_, S>) -> Result<(Bytes, ()), Error> {
        let mut bytes = view.into_view();
        bytes.value.extend_from_slice(&self.0);
        Ok((bytes, ()))
    }
}
fn ready<T>(submission: Result<Submission<T>, Rejected<Request>>) -> T {
    match submission.map_err(|r| r.reason).unwrap() {
        Submission::Ready(Ok(Outcome::Success(value))) => value,
        _ => panic!("Expected synchronous success"),
    }
}
#[test]
fn reads_and_copy_updates_transfer_decoded_ownership_without_cloning() {
    let clones = Arc::new(AtomicUsize::new(0));
    let store = RasterKV::builder(SchemaPair::new(
        U64Key,
        SerializedValue::new(Codec(clones.clone())),
    ))
    .device(Box::new(crate::device::null::NullDeviceFactory))
    .create()
    .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    ready(session.upsert(Serial(0), Request(vec![7, 8], clones.clone())));
    let first = ready(session.read(
        Serial(1),
        Request(vec![], clones.clone()),
        ReadOptions::default(),
    ));
    ready(session.rmw(
        Serial(2),
        Request(vec![0xff], clones.clone()),
        RmwOptions::default(),
    ));
    let second = ready(session.read(
        Serial(3),
        Request(vec![], clones.clone()),
        ReadOptions::default(),
    ));
    drop(session);
    drop(store);
    assert_eq!(first, vec![7, 8]);
    assert_eq!(second, vec![7, 8, 0xff]);
    assert_eq!(clones.load(Ordering::SeqCst), 0);
}
