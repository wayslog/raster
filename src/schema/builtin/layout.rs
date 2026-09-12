//! Built-in layouts are only accessible within the permission provided by the record owner;Ordinary slot headers are in-process metadata.
use super::*;
use crate::schema::value::*;
use std::{
    ptr::NonNull,
    sync::atomic::{AtomicU64, Ordering},
};

fn check(pointer: NonNull<u8>, len: usize, needed: usize, alignment: usize) -> Result<(), Error> {
    if len < needed || !(pointer.as_ptr() as usize).is_multiple_of(alignment) {
        return Err(Error::Codec(
            "Insufficient value slot capacity or address alignment",
        ));
    }
    Ok(())
}
// SAFETY: The caller holds read or exclusive permission covering the entire scope,And the normal layout has been successfully initialized.
unsafe fn encoded<'a>(pointer: NonNull<u8>, len: usize) -> Result<&'a [u8], Error> {
    check(pointer, len, 8, 8)?;
    // SAFETY: Header is within license and initialized,Use little-endian reads to avoid relying on host endianness.
    let all = unsafe { std::slice::from_raw_parts(pointer.as_ptr(), len) };
    let n = usize::try_from(u64::from_le_bytes(
        all[..8].try_into().expect("eight-byte header"),
    ))
    .map_err(|_| Error::CapacityExceeded)?;
    all.get(8..8usize.checked_add(n).ok_or(Error::CapacityExceeded)?)
        .ok_or(Error::Codec("The length in the slot is out of bounds"))
}
// SAFETY: The caller holds an exclusive license covering the entire scope;No other access aliases.
unsafe fn write(pointer: NonNull<u8>, len: usize, bytes: &[u8]) -> Result<(), Error> {
    let needed = bytes.len().checked_add(8).ok_or(Error::CapacityExceeded)?;
    check(pointer, len, needed, 8)?;
    // SAFETY: Dimensional alignment verified,The caller provides exclusivity,The output does not overlap with the owned temporary encoding.
    let all = unsafe { std::slice::from_raw_parts_mut(pointer.as_ptr(), len) };
    all[..8].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
    all[8..needed].copy_from_slice(bytes);
    all[needed..].fill(0);
    Ok(())
}
pub struct SerializedUpdate<'a, C: ValueCodec> {
    codec: &'a C,
    permit: UpdatePermit<'a>,
}
impl<C: ValueCodec> SerializedUpdate<'_, C> {
    pub fn read_owned(&self) -> Result<C::Value, Error> {
        // SAFETY: Current view holds exclusive update license,Ordinary byte slot has been initialized.
        self.codec
            .decode(unsafe { encoded(self.permit.as_ptr(), self.permit.len())? })
    }
    pub fn replace(&mut self, value: &C::Value) -> Result<(), Error> {
        let bytes = self.codec.encode(value)?;
        // SAFETY: Holds an exclusive license;Complete coding first,Capacity failure does not write any bytes.
        unsafe { write(self.permit.as_ptr(), self.permit.len(), &bytes) }
    }
}
// SAFETY: Ordinary byte access uses record exclusive arbitration,View does not escape permission;Coding is done first,Do not change slots before failure.
unsafe impl<C: ValueCodec> ValueLayout for SerializedValue<C> {
    type Owned = C::Value;
    type Read<'a> = C::Value;
    type Update<'a> = SerializedUpdate<'a, C>;
    fn format_id(&self) -> FormatId {
        self.codec.format_id()
    }
    fn plan(&self, value: &C::Value) -> Result<ValuePlan, Error> {
        Ok(self.prepare(value)?.plan())
    }
    fn decode_owned(&self, bytes: &[u8]) -> Result<Self::Owned, Error> {
        self.codec.decode(bytes)
    }
    fn plan_decode(&self, bytes: &[u8]) -> Result<ValuePlan, Error> {
        let value = self.codec.decode(bytes)?;
        let mut plan = self.plan(&value)?;
        plan.encoded_bytes = bytes.len();
        plan.capacity = plan.capacity.max(bytes.len());
        plan.validate()
    }
    fn initialize(&self, p: InitPermit<'_>, value: C::Value) -> Result<(), Error> {
        let bytes = self.codec.encode(&value)?;
        // SAFETY: The initialization permission of an unreleased slot is exclusive to the entire scope.
        unsafe { write(p.as_ptr(), p.len(), &bytes) }
    }
    fn read<'a>(&'a self, p: ReadPermit<'a>) -> Result<C::Value, Error> {
        // SAFETY: The record owner excludes ordinary updates and ensures that the slot is alive and initialized.
        self.codec.decode(unsafe { encoded(p.as_ptr(), p.len())? })
    }
    fn update<'a>(&'a self, p: UpdatePermit<'a>) -> Result<Self::Update<'a>, Error> {
        check(p.as_ptr(), p.len(), 8, 8)?;
        Ok(SerializedUpdate {
            codec: &self.codec,
            permit: p,
        })
    }
    fn stable_encoded_len(&self, p: StablePermit<'_>) -> Result<usize, Error> {
        // SAFETY: Stable license excludes modifications,Slot has been initialized,The length prefix is bounds checked.
        Ok(unsafe { encoded(p.as_ptr(), p.len())? }.len())
    }
    fn encode_stable(&self, p: StablePermit<'_>, output: &mut [u8]) -> Result<(), Error> {
        // SAFETY: Stable license excludes modifications and guarantees full active byte slots.
        let bytes = unsafe { encoded(p.as_ptr(), p.len())? };
        encode_exact(bytes, output)
    }
    fn decode_initialize(&self, bytes: &[u8], p: InitPermit<'_>) -> Result<(), Error> {
        let owned = self.codec.decode(bytes)?;
        self.initialize(p, owned)
    }
    fn drop_value(&self, _: DropPermit<'_>) -> Result<(), Error> {
        Ok(())
    }
}
// SAFETY: Only initialize/Destroy exclusive access,All subsequent numeric accesses use AtomicU64 Atomic operations.
unsafe impl ValueLayout for AtomicU64Value {
    type Owned = u64;
    type Read<'a> = u64;
    type Update<'a> = &'a AtomicU64;
    fn concurrent_updates(&self) -> bool {
        true
    }
    fn format_id(&self) -> FormatId {
        self.format_id()
    }
    fn plan(&self, value: &u64) -> Result<ValuePlan, Error> {
        Ok(self.prepare(*value)?.plan())
    }
    fn decode_owned(&self, bytes: &[u8]) -> Result<Self::Owned, Error> {
        AtomicU64Value::decode_owned(self, bytes)
    }
    fn plan_decode(&self, bytes: &[u8]) -> Result<ValuePlan, Error> {
        self.plan(&self.decode_owned(bytes)?)
    }
    fn initialize(&self, p: InitPermit<'_>, value: u64) -> Result<(), Error> {
        check(
            p.as_ptr(),
            p.len(),
            size_of::<AtomicU64>(),
            align_of::<AtomicU64>(),
        )?;
        // SAFETY: Initial license exclusive,address satisfies AtomicU64 Alignment and size,No active atomic objects yet.
        unsafe {
            p.as_ptr()
                .cast::<AtomicU64>()
                .as_ptr()
                .write(AtomicU64::new(value))
        };
        Ok(())
    }
    fn read<'a>(&'a self, p: ReadPermit<'a>) -> Result<u64, Error> {
        check(
            p.as_ptr(),
            p.len(),
            size_of::<AtomicU64>(),
            align_of::<AtomicU64>(),
        )?;
        // SAFETY: The atomic unit is initialized and the license is guaranteed to be alive.;No normal byte reading.
        Ok(unsafe { p.as_ptr().cast::<AtomicU64>().as_ref() }.load(Ordering::SeqCst))
    }
    fn update<'a>(&'a self, p: UpdatePermit<'a>) -> Result<&'a AtomicU64, Error> {
        check(
            p.as_ptr(),
            p.len(),
            size_of::<AtomicU64>(),
            align_of::<AtomicU64>(),
        )?;
        // SAFETY: Initialized atomic unit,Return references limited to license lifetime,Atomic updates allow sharing.
        Ok(unsafe { p.as_ptr().cast::<AtomicU64>().as_ref() })
    }
    fn stable_encoded_len(&self, p: StablePermit<'_>) -> Result<usize, Error> {
        check(
            p.as_ptr(),
            p.len(),
            size_of::<AtomicU64>(),
            align_of::<AtomicU64>(),
        )?;
        Ok(8)
    }
    fn encode_stable(&self, p: StablePermit<'_>, output: &mut [u8]) -> Result<(), Error> {
        check(
            p.as_ptr(),
            p.len(),
            size_of::<AtomicU64>(),
            align_of::<AtomicU64>(),
        )?;
        // SAFETY: Stable permissions ensure atomic object survival;Read only logical values.
        let value = unsafe { p.as_ptr().cast::<AtomicU64>().as_ref() }.load(Ordering::SeqCst);
        encode_exact(&value.to_le_bytes(), output)
    }
    fn decode_initialize(&self, bytes: &[u8], p: InitPermit<'_>) -> Result<(), Error> {
        self.initialize(p, self.decode_owned(bytes)?)
    }
    fn drop_value(&self, p: DropPermit<'_>) -> Result<(), Error> {
        check(
            p.as_ptr(),
            p.len(),
            size_of::<AtomicU64>(),
            align_of::<AtomicU64>(),
        )?;
        // SAFETY: Last owner's permission to destroy,No concurrent access and initialized,Destroyed only once.
        unsafe { std::ptr::drop_in_place(p.as_ptr().cast::<AtomicU64>().as_ptr()) };
        Ok(())
    }
}
