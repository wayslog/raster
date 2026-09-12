//! The benchmark request directly calls the public interface;Integer and variable-length layouts retain the true update path respectively.
use crate::{model::ResultValue, trace::Value};
use raster::{
    api::operation::*,
    schema::{ValueLayout, ValueRead, ValueUpdate, builtin::*},
    types::Error,
};
use std::{marker::PhantomData, sync::atomic::Ordering};
pub type Schema<K> = SchemaPair<U64Key, <K as Kind>::Layout>;
type Owned<K> = <<K as Kind>::Layout as ValueLayout>::Owned;
pub trait Kind: Sized + Send + Sync + 'static {
    type Layout: ValueLayout;
    fn layout() -> Self::Layout;
    fn owned(value: &Value) -> Result<Owned<Self>, Error>;
    fn read(view: ValueRead<'_, Schema<Self>>) -> Result<Value, Error>;
    fn update(
        view: ValueUpdate<'_, Schema<Self>>,
        value: &Value,
        rmw: bool,
    ) -> Result<UpdateDecision<ResultValue>, Error>;
}
pub struct Fixed;
impl Kind for Fixed {
    type Layout = AtomicU64Value;
    fn layout() -> Self::Layout {
        AtomicU64Value
    }
    fn owned(value: &Value) -> Result<u64, Error> {
        match value {
            Value::Number(n) => Ok(*n),
            _ => Err(Error::Codec("Base integer type mismatch")),
        }
    }
    fn read(view: ValueRead<'_, Schema<Self>>) -> Result<Value, Error> {
        Ok(Value::Number(*view.view()))
    }
    fn update(
        mut view: ValueUpdate<'_, Schema<Self>>,
        value: &Value,
        rmw: bool,
    ) -> Result<UpdateDecision<ResultValue>, Error> {
        let input = Self::owned(value)?;
        let atomic = *view.view_mut();
        let output = if rmw {
            ResultValue::Value(Value::Number(
                atomic
                    .fetch_add(input, Ordering::SeqCst)
                    .wrapping_add(input),
            ))
        } else {
            atomic.store(input, Ordering::SeqCst);
            ResultValue::Written
        };
        Ok(UpdateDecision::Updated(output))
    }
}
pub struct Variable;
impl Kind for Variable {
    type Layout = SerializedValue<ByteValueCodec>;
    fn layout() -> Self::Layout {
        SerializedValue::new(ByteValueCodec)
    }
    fn owned(value: &Value) -> Result<Vec<u8>, Error> {
        match value {
            Value::Bytes(bytes) => Ok(bytes.clone()),
            _ => Err(Error::Codec("base byte type mismatch")),
        }
    }
    fn read(view: ValueRead<'_, Schema<Self>>) -> Result<Value, Error> {
        Ok(Value::Bytes(view.view().clone()))
    }
    fn update(
        mut view: ValueUpdate<'_, Schema<Self>>,
        value: &Value,
        rmw: bool,
    ) -> Result<UpdateDecision<ResultValue>, Error> {
        let input = Self::owned(value)?;
        let next = if rmw {
            let mut old = view.view_mut().read_owned()?;
            old.extend_from_slice(&input);
            old
        } else {
            input
        };
        match view.view_mut().replace(&next) {
            Ok(()) => Ok(UpdateDecision::Updated(if rmw {
                ResultValue::Value(Value::Bytes(next))
            } else {
                ResultValue::Written
            })),
            Err(Error::Codec(_)) => Ok(UpdateDecision::Append),
            Err(error) => Err(error),
        }
    }
}
pub struct Request<K: Kind> {
    key: u64,
    value: Value,
    kind: PhantomData<K>,
}
impl<K: Kind> Request<K> {
    pub fn new(key: u64, value: Value) -> Self {
        Self {
            key,
            value,
            kind: PhantomData,
        }
    }
}
impl<K: Kind> Keyed<Schema<K>> for Request<K> {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl<K: Kind> ReadOperation<Schema<K>> for Request<K> {
    type Output = ResultValue;
    fn read(&mut self, view: ValueRead<'_, Schema<K>>) -> Result<ResultValue, Error> {
        Ok(ResultValue::Value(K::read(view)?))
    }
}
impl<K: Kind> UpsertOperation<Schema<K>> for Request<K> {
    type Output = ResultValue;
    fn replacement(&mut self) -> Result<(Owned<K>, ResultValue), Error> {
        Ok((K::owned(&self.value)?, ResultValue::Written))
    }
    fn update_in_place(
        &mut self,
        view: ValueUpdate<'_, Schema<K>>,
    ) -> Result<UpdateDecision<ResultValue>, Error> {
        K::update(view, &self.value, false)
    }
}
impl<K: Kind> RmwOperation<Schema<K>> for Request<K> {
    type Output = ResultValue;
    fn initial(&mut self) -> Result<(Owned<K>, ResultValue), Error> {
        Ok((
            K::owned(&self.value)?,
            ResultValue::Value(self.value.clone()),
        ))
    }
    fn copy_update(
        &mut self,
        view: ValueRead<'_, Schema<K>>,
    ) -> Result<(Owned<K>, ResultValue), Error> {
        let next = match (K::read(view)?, &self.value) {
            (Value::Number(old), Value::Number(n)) => Value::Number(old.wrapping_add(*n)),
            (Value::Bytes(mut old), Value::Bytes(bytes)) => {
                old.extend_from_slice(bytes);
                Value::Bytes(old)
            }
            _ => return Err(Error::Codec("benchmark RMW type mismatch")),
        };
        Ok((K::owned(&next)?, ResultValue::Value(next)))
    }
    fn update_in_place(
        &mut self,
        view: ValueUpdate<'_, Schema<K>>,
    ) -> Result<UpdateDecision<ResultValue>, Error> {
        K::update(view, &self.value, true)
    }
}
impl<K: Kind> DeleteOperation<Schema<K>> for Request<K> {
    type Output = ResultValue;
    fn complete(self, _: DeleteOutcome) -> ResultValue {
        ResultValue::Deleted
    }
}
