//! Example has request and output;Variable length updates are calculated first and then appended.,Errors will not be repeated if changes have taken effect..
use raster::{
    api::operation::*,
    schema::{
        ValueRead, ValueUpdate,
        builtin::{ByteKey, ByteValueCodec, SchemaPair, SerializedValue},
    },
    types::Error,
};
pub type Schema = SchemaPair<ByteKey, SerializedValue<ByteValueCodec>>;
#[derive(Debug)]
pub struct Request {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
impl Request {
    pub fn key(key: Vec<u8>) -> Self {
        Self {
            key,
            value: Vec::new(),
        }
    }
}
impl Keyed<Schema> for Request {
    fn key(&self) -> &[u8] {
        &self.key
    }
}
impl ReadOperation<Schema> for Request {
    type Output = Vec<u8>;
    fn read(&mut self, value: ValueRead<'_, Schema>) -> Result<Self::Output, Error> {
        Ok(value.view().clone())
    }
}
impl UpsertOperation<Schema> for Request {
    type Output = ();
    fn replacement(&mut self) -> Result<(Vec<u8>, ()), Error> {
        Ok((self.value.clone(), ()))
    }
    fn update_in_place(&mut self, _: ValueUpdate<'_, Schema>) -> Result<UpdateDecision<()>, Error> {
        Ok(UpdateDecision::Append)
    }
}
impl RmwOperation<Schema> for Request {
    type Output = Vec<u8>;
    fn initial(&mut self) -> Result<(Vec<u8>, Vec<u8>), Error> {
        Ok((self.value.clone(), self.value.clone()))
    }
    fn copy_update(&mut self, old: ValueRead<'_, Schema>) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let mut value = old.view().clone();
        value
            .try_reserve(self.value.len())
            .map_err(|_| Error::OutOfMemory)?;
        value.extend_from_slice(&self.value);
        Ok((value.clone(), value))
    }
    fn update_in_place(
        &mut self,
        _: ValueUpdate<'_, Schema>,
    ) -> Result<UpdateDecision<Vec<u8>>, Error> {
        Ok(UpdateDecision::Append)
    }
}
impl DeleteOperation<Schema> for Request {
    type Output = ();
    fn complete(self, _: DeleteOutcome) {}
}
