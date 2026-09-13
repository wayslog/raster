use super::*;
use crate::{
    RasterKV, Submission,
    api::{Outcome, operation::*},
    schema::{ValueRead, ValueUpdate},
    types::*,
};
use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
#[derive(Clone)]
struct Codec {
    owned: Arc<AtomicUsize>,
    views: Arc<AtomicUsize>,
}
impl ValueCodec for Codec {
    type Value = Vec<u8>;
    fn format_id(&self) -> FormatId {
        ByteValueCodec.format_id()
    }
    fn encode(&self, value: &Vec<u8>) -> Result<Vec<u8>, Error> {
        self.owned.fetch_add(1, Ordering::SeqCst);
        ByteValueCodec.encode(value)
    }
    fn encode_view<'a>(&self, value: &'a Vec<u8>) -> Result<Cow<'a, [u8]>, Error> {
        self.views.fetch_add(1, Ordering::SeqCst);
        ByteValueCodec.encode_view(value)
    }
    fn decode(&self, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        ByteValueCodec.decode(bytes)
    }
}
type S = SchemaPair<U64Key, SerializedValue<Codec>>;
struct Request(Vec<u8>);
impl Keyed<S> for Request {
    fn key(&self) -> &u64 {
        &1
    }
}
impl UpsertOperation<S> for Request {
    type Output = ();
    fn replacement(&mut self) -> Result<(Vec<u8>, ()), Error> {
        Ok((self.0.clone(), ()))
    }
    fn update_in_place(
        &mut self,
        mut value: ValueUpdate<'_, S>,
    ) -> Result<UpdateDecision<()>, Error> {
        value.view_mut().replace(&self.0)?;
        Ok(UpdateDecision::Updated(()))
    }
}
impl ReadOperation<S> for Request {
    type Output = Vec<u8>;
    fn read(&mut self, value: ValueRead<'_, S>) -> Result<Vec<u8>, Error> {
        Ok(value.view().clone())
    }
}
impl RmwOperation<S> for Request {
    type Output = ();
    fn initial(&mut self) -> Result<(Vec<u8>, ()), Error> {
        Ok((self.0.clone(), ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, S>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
    fn copy_update(&mut self, value: ValueRead<'_, S>) -> Result<(Vec<u8>, ()), Error> {
        let mut next = value.view().clone();
        next.extend_from_slice(&self.0);
        Ok((next, ()))
    }
}
fn ready<T>(value: Result<Submission<T>, Rejected<Request>>) -> T {
    match value.map_err(|r| r.reason).unwrap() {
        Submission::Ready(Ok(Outcome::Success(value))) => value,
        _ => panic!("Expected synchronous success"),
    }
}
#[test]
fn live_variable_operations_use_encoding_views_without_owned_encoding_buffers() {
    let codec = Codec {
        owned: Arc::new(AtomicUsize::new(0)),
        views: Arc::new(AtomicUsize::new(0)),
    };
    let store = RasterKV::builder(SchemaPair::new(U64Key, SerializedValue::new(codec.clone())))
        .device(Box::new(crate::device::null::NullDeviceFactory))
        .create()
        .unwrap();
    let mut session = store.start_session(Default::default()).unwrap();
    ready(session.upsert(Serial(0), Request(vec![7, 8])));
    ready(session.upsert(Serial(1), Request(vec![9, 10])));
    ready(session.rmw(Serial(2), Request(vec![11]), RmwOptions::default()));
    assert_eq!(
        ready(session.read(Serial(3), Request(vec![]), ReadOptions::default())),
        vec![9, 10, 11]
    );
    assert_eq!(codec.owned.load(Ordering::SeqCst), 0);
    assert_eq!(codec.views.load(Ordering::SeqCst), 5);
}

#[test]
fn byte_encoding_borrows_input_while_prepared_values_remain_independent() {
    for mut input in [vec![], vec![0, 0xff, 1], vec![7; 4097]] {
        let view = ByteValueCodec.encode_view(&input).unwrap();
        assert!(matches!(view, Cow::Borrowed(_)));
        assert_eq!(view.as_ptr(), input.as_ptr());
        assert_eq!(&*view, input);
        let layout = SerializedValue::new(ByteValueCodec);
        let prepared = layout.prepare(&input).unwrap();
        let plan = layout.plan(&input).unwrap();
        assert_eq!(plan.live_bytes, prepared.plan().live_bytes);
        assert_eq!(plan.encoded_bytes, input.len());
        assert_eq!(plan.capacity, prepared.plan().capacity);
        assert_eq!(plan.alignment, prepared.plan().alignment);
        let expected = input.clone();
        input.fill(42);
        assert_eq!(prepared.bytes(), expected);
    }
}
