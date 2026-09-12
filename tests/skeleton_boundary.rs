//! for testing Schema No access to raw memory,Only validate the skeleton's type and rejection bounds.
use raster::{
    RasterKV,
    api::{
        maintenance::RecoverySet,
        operation::{Keyed, ReadOperation},
    },
    device::*,
    schema::{KeyCodec, ValueRead, builtin::SchemaPair, value::*},
    types::*,
};
use std::{
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct TestKey;
impl KeyCodec for TestKey {
    type Key = u64;
    type OwnedKey = u64;
    fn format_id(&self) -> FormatId {
        FormatId([1; 16])
    }
    fn hash_descriptor(&self) -> HashDescriptor {
        HashDescriptor {
            algorithm: FormatId([2; 16]),
            seed: vec![],
        }
    }
    fn hash(&self, key: &u64) -> KeyHash {
        KeyHash(*key)
    }
    fn encoded_len(&self, _key: &u64) -> Result<u32, Error> {
        Ok(8)
    }
    fn encode(&self, key: &u64, output: &mut [u8]) -> Result<(), Error> {
        if output.len() != 8 {
            return Err(Error::Codec("Test key length error"));
        }
        output.copy_from_slice(&key.to_le_bytes());
        Ok(())
    }
    fn equals_encoded(&self, key: &u64, encoded: &[u8]) -> Result<bool, Error> {
        Ok(*key == self.decode_owned(encoded)?)
    }
    fn decode_owned(&self, encoded: &[u8]) -> Result<u64, Error> {
        let bytes = encoded
            .try_into()
            .map_err(|_| Error::Codec("Test key length error"))?;
        Ok(u64::from_le_bytes(bytes))
    }
}
struct TestLayout;
// SAFETY: Test layout all permission related methods rejected,Do not dereference pointers,Do not construct record view.
unsafe impl ValueLayout for TestLayout {
    type Owned = u64;
    type Read<'a> = &'a u64;
    type Update<'a> = &'a mut u64;
    fn format_id(&self) -> FormatId {
        FormatId([3; 16])
    }
    fn plan(&self, _value: &u64) -> Result<ValuePlan, Error> {
        Ok(ValuePlan {
            live_bytes: 8,
            encoded_bytes: 8,
            capacity: 8,
            alignment: 8,
        })
    }
    fn decode_owned(&self, bytes: &[u8]) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
            Error::Codec("Integers require eight bytes")
        })?))
    }
    fn plan_decode(&self, _: &[u8]) -> Result<ValuePlan, Error> {
        Err(Error::unimplemented("test_layout"))
    }
    fn initialize(&self, _p: InitPermit<'_>, _v: u64) -> Result<(), Error> {
        Err(Error::unimplemented("test_layout"))
    }
    fn read<'a>(&'a self, _p: ReadPermit<'a>) -> Result<Self::Read<'a>, Error> {
        Err(Error::unimplemented("test_layout"))
    }
    fn update<'a>(&'a self, _p: UpdatePermit<'a>) -> Result<Self::Update<'a>, Error> {
        Err(Error::unimplemented("test_layout"))
    }
    fn stable_encoded_len(&self, _: StablePermit<'_>) -> Result<usize, Error> {
        Err(Error::Codec("Test layout does not support stable encoding"))
    }
    fn encode_stable(&self, _p: StablePermit<'_>, _out: &mut [u8]) -> Result<(), Error> {
        Err(Error::unimplemented("test_layout"))
    }
    fn decode_initialize(&self, _input: &[u8], _p: InitPermit<'_>) -> Result<(), Error> {
        Err(Error::unimplemented("test_layout"))
    }
    fn drop_value(&self, _p: DropPermit<'_>) -> Result<(), Error> {
        Err(Error::unimplemented("test_layout"))
    }
}
type TestSchema = SchemaPair<TestKey, TestLayout>;
struct SpyFactory(Arc<AtomicUsize>);
impl DeviceFactory for SpyFactory {
    fn open(&self, _options: DeviceOpenOptions) -> Result<Box<dyn Device>, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(Error::unimplemented("Test equipment"))
    }
}

#[test]
fn creating_delivery_device_fails_with_invalid_recovery_identity_does_not_open_device() {
    let calls = Arc::new(AtomicUsize::new(0));
    let build = || {
        RasterKV::builder(SchemaPair::new(TestKey, TestLayout))
            .device(Box::new(SpyFactory(Arc::clone(&calls))))
    };
    assert!(matches!(
        build().create(),
        Err(Error::NotImplemented {
            module: "Test equipment"
        })
    ));
    let set = RecoverySet {
        store: StoreId([0; 16]),
        index: CheckpointToken([1; 16]),
        log: CheckpointToken([2; 16]),
    };
    assert!(matches!(build().recover(set), Err(Error::InvalidFormat(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct LocalRead {
    key: u64,
    result: Rc<String>,
}
impl Keyed<TestSchema> for LocalRead {
    fn key(&self) -> &u64 {
        &self.key
    }
}
impl ReadOperation<TestSchema> for LocalRead {
    type Output = Rc<String>;
    fn read(&mut self, _value: ValueRead<'_, TestSchema>) -> Result<Self::Output, Error> {
        Ok(Rc::clone(&self.result))
    }
}
#[test]
fn the_shared_engine_and_this_thread_context_types_can_be_established_at_the_same_time() {
    fn shared<T: Send + Sync>() {}
    fn local<O: ReadOperation<TestSchema>>(_operation: O) {}
    shared::<RasterKV<TestSchema>>();
    local(LocalRead {
        key: 1,
        result: Rc::new(String::from("Output of this thread")),
    });
}
