//! Value Expert Extended Compact.License cannot be constructed from secure external code,Do not directly expose shared mutable references.
use crate::schema::Schema;
use crate::types::{Error, FormatId, Generation};
use std::{marker::PhantomData, ptr::NonNull, rc::Rc};

#[derive(Clone, Copy, Debug)]
pub struct ValuePlan {
    pub live_bytes: usize,
    pub encoded_bytes: usize,
    pub capacity: usize,
    pub alignment: usize,
}
impl ValuePlan {
    pub fn validate(self) -> Result<Self, Error> {
        if !self.alignment.is_power_of_two()
            || self.live_bytes > self.capacity
            || self.encoded_bytes > self.capacity
            || self.capacity > u32::MAX as usize
            || std::alloc::Layout::from_size_align(self.capacity, self.alignment).is_err()
        {
            return Err(Error::Codec(
                "Value size or alignment does not meet slot capacity",
            ));
        }
        Ok(self)
    }
}

/// Canonical encoding strategy for common values. Encoding returns owned temporary bytes;
/// errors never modify the shared slot.
/// The implementation must stably encode the same logical value,refuse to tail/corrupted input;Does not encode process pointers or locks.
pub trait ValueCodec: Send + Sync + 'static {
    type Value: Send + Sync + 'static;
    fn format_id(&self) -> FormatId;
    fn encode(&self, value: &Self::Value) -> Result<Vec<u8>, Error>;
    fn decode(&self, bytes: &[u8]) -> Result<Self::Value, Error>;
}

/// The object can only be obtained after the encoding is completed;it holds temporary bytes,Page access not granted.
pub struct PreparedValue {
    bytes: Vec<u8>,
    plan: ValuePlan,
}
impl PreparedValue {
    pub fn new(bytes: Vec<u8>, live_bytes: usize, alignment: usize) -> Result<Self, Error> {
        let plan = ValuePlan {
            live_bytes,
            encoded_bytes: bytes.len(),
            capacity: live_bytes.max(bytes.len()),
            alignment,
        }
        .validate()?;
        Ok(Self { bytes, plan })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn plan(&self) -> ValuePlan {
        self.plan
    }
    /// Check the numerical bounds of candidate slots;Success does not prove that the address is alive or has exclusive access.
    pub fn fits(&self, capacity: usize, alignment: usize) -> Result<(), Error> {
        ValuePlan {
            capacity,
            ..self.plan
        }
        .validate()?;
        if !alignment.is_power_of_two() || alignment < self.plan.alignment {
            return Err(Error::Codec("Insufficient slot alignment"));
        }
        Ok(())
    }
}

macro_rules! permit {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        pub struct $name<'a> {
            pub(crate) pointer: NonNull<u8>,
            pub(crate) length: usize,
            pub(crate) generation: Generation,
            pub(crate) guard: PhantomData<&'a ()>,
            pub(crate) local: PhantomData<Rc<()>>,
        }
        impl $name<'_> {
            pub fn as_ptr(&self) -> NonNull<u8> {
                self.pointer
            }
            pub fn len(&self) -> usize {
                self.length
            }
            pub fn is_empty(&self) -> bool {
                self.length == 0
            }
            pub fn generation(&self) -> Generation {
                self.generation
            }
        }
    };
}
permit!(
    InitPermit,
    "Exclusive initialization license for unreleased records."
);
permit!(
    ReadPermit,
    "Short-term read permission with survival protection,Does not mean exclusive modification."
);
permit!(
    UpdatePermit,
    "Short-term update permissions written to quorum via record."
);
permit!(
    StablePermit,
    "Concurrent updates excluded,License to generate disk images."
);
permit!(
    DropPermit,
    "Exclusive destruction of license after last access and device reference has ended."
);

/// The active value represents the conversion interface to the disk format.
///
/// Atomic update view cannot exceed license lifetime:
/// ```compile_fail
/// use raster::schema::{ValueLayout, builtin::AtomicU64Value, value::UpdatePermit};
/// fn escape<'a>(layout: &'a AtomicU64Value, permit: UpdatePermit<'a>)
///     -> &'static std::sync::atomic::AtomicU64 {
///     layout.update(permit).unwrap()
/// }
/// ```
///
/// # Safety
/// Implementations must verify size and alignment,Access only within permission scope;Returned views must not exceed license lifetime.
/// Concurrent atomic accesses must not be mixed with normal byte reads;Initialization failure must be cleanable,Destroy without duplication.
/// Insufficient capacity or failure shall not leave shared half value;Disk bytes cannot be restored to process pointer or lock state.
/// initialize/decode_initialize Return Err or expand panic Before destroying its own initialized sub-objects,
/// Return the slot to its uninitialized state;The caller can only reclaim the original slot at this time,Not allowed to be called drop_value.
/// Called exactly once by the page owner upon success drop_value.Decoding should be completed first with the owned temporary,rewrite slot.
pub unsafe trait ValueLayout: Send + Sync + 'static {
    type Owned: Send + Sync + 'static;
    type Read<'a>
    where
        Self: 'a;
    type Update<'a>
    where
        Self: 'a;
    /// Return true The expert layout must ensure multiple Update View simultaneous access security.
    fn concurrent_updates(&self) -> bool {
        false
    }
    fn format_id(&self) -> FormatId;
    fn plan(&self, value: &Self::Owned) -> Result<ValuePlan, Error>;
    /// Plan active objects according to stable coding;Disk slot capacity cannot be directly used as active layout.
    fn plan_decode(&self, encoded: &[u8]) -> Result<ValuePlan, Error>;
    /// Decode stable encodings into independently owned values;No borrowed input is allowed,Log page or runtime slot.
    /// The semantics must be the same as decode_initialize/read consistent,For scanning to return a saved value that can be recycled across pages.
    fn decode_owned(&self, encoded: &[u8]) -> Result<Self::Owned, Error>;
    fn initialize(&self, permit: InitPermit<'_>, value: Self::Owned) -> Result<(), Error>;
    fn read<'a>(&'a self, permit: ReadPermit<'a>) -> Result<Self::Read<'a>, Error>;
    fn update<'a>(&'a self, permit: UpdatePermit<'a>) -> Result<Self::Update<'a>, Error>;
    /// Returns the current encoding length within a stable license;must be followed encode_stable The exact output length of.
    fn stable_encoded_len(&self, permit: StablePermit<'_>) -> Result<usize, Error>;
    fn encode_stable(&self, permit: StablePermit<'_>, output: &mut [u8]) -> Result<(), Error>;
    fn decode_initialize(&self, encoded: &[u8], permit: InitPermit<'_>) -> Result<(), Error>;
    fn drop_value(&self, permit: DropPermit<'_>) -> Result<(), Error>;
}

/// View borrow cannot escape by returning reference to owning output.
///
/// ```compile_fail
/// use raster::schema::{Schema, ValueLayout, ValueRead};
/// fn escape<'a, S: Schema>(value: ValueRead<'a, S>)
///     -> &'static <S::Value as ValueLayout>::Read<'a>
/// {
///     value.view()
/// }
/// ```
pub struct ValueRead<'a, S: Schema> {
    pub(crate) view: <S::Value as ValueLayout>::Read<'a>,
}
impl<'a, S: Schema> ValueRead<'a, S> {
    pub fn view(&self) -> &<S::Value as ValueLayout>::Read<'a> {
        &self.view
    }
}
pub struct ValueUpdate<'a, S: Schema> {
    pub(crate) view: <S::Value as ValueLayout>::Update<'a>,
}
impl<'a, S: Schema> ValueUpdate<'a, S> {
    pub fn view_mut(&mut self) -> &mut <S::Value as ValueLayout>::Update<'a> {
        &mut self.view
    }
}
