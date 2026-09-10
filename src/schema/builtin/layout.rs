//! 内建布局只在记录所有者提供的许可内访问；普通槽头是进程内元数据。
use super::*;
use crate::schema::value::*;
use std::{
    ptr::NonNull,
    sync::atomic::{AtomicU64, Ordering},
};

fn check(pointer: NonNull<u8>, len: usize, needed: usize, alignment: usize) -> Result<(), Error> {
    if len < needed || !(pointer.as_ptr() as usize).is_multiple_of(alignment) {
        return Err(Error::Codec("值槽容量或地址对齐不足"));
    }
    Ok(())
}
// SAFETY: 调用者持有覆盖整个范围的读或独占许可，且普通布局已成功初始化。
unsafe fn encoded<'a>(pointer: NonNull<u8>, len: usize) -> Result<&'a [u8], Error> {
    check(pointer, len, 8, 8)?;
    // SAFETY: 头部在许可内且已初始化，使用字节小端读取避免依赖主机端序。
    let all = unsafe { std::slice::from_raw_parts(pointer.as_ptr(), len) };
    let n = usize::try_from(u64::from_le_bytes(all[..8].try_into().expect("八字节头部")))
        .map_err(|_| Error::CapacityExceeded)?;
    all.get(8..8usize.checked_add(n).ok_or(Error::CapacityExceeded)?)
        .ok_or(Error::Codec("槽内长度越界"))
}
// SAFETY: 调用者持有覆盖整个范围的独占许可；无其他访问别名。
unsafe fn write(pointer: NonNull<u8>, len: usize, bytes: &[u8]) -> Result<(), Error> {
    let needed = bytes.len().checked_add(8).ok_or(Error::CapacityExceeded)?;
    check(pointer, len, needed, 8)?;
    // SAFETY: 尺寸对齐已验证，调用者提供独占权，输出与拥有型临时编码不重叠。
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
        // SAFETY: 当前视图持独占更新许可，普通字节槽已初始化。
        self.codec
            .decode(unsafe { encoded(self.permit.as_ptr(), self.permit.len())? })
    }
    pub fn replace(&mut self, value: &C::Value) -> Result<(), Error> {
        let bytes = self.codec.encode(value)?;
        // SAFETY: 持有独占许可；先完成编码，容量失败不写任何字节。
        unsafe { write(self.permit.as_ptr(), self.permit.len(), &bytes) }
    }
}
// SAFETY: 普通字节访问走记录独占仲裁，视图不逃逸许可；编码先完成，失败前不改槽。
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
    fn initialize(&self, p: InitPermit<'_>, value: C::Value) -> Result<(), Error> {
        let bytes = self.codec.encode(&value)?;
        // SAFETY: 未发布槽的初始化许可独占整个范围。
        unsafe { write(p.as_ptr(), p.len(), &bytes) }
    }
    fn read<'a>(&'a self, p: ReadPermit<'a>) -> Result<C::Value, Error> {
        // SAFETY: 记录所有者排除普通更新并保证槽存活且已初始化。
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
        // SAFETY: 稳定许可排除修改，槽已经初始化，长度前缀经过边界检查。
        Ok(unsafe { encoded(p.as_ptr(), p.len())? }.len())
    }
    fn encode_stable(&self, p: StablePermit<'_>, output: &mut [u8]) -> Result<(), Error> {
        // SAFETY: 稳定许可排除修改并保证完整活跃字节槽。
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
// SAFETY: 仅初始化/销毁独占访问，之后所有数值访问均使用 AtomicU64 的原子操作。
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
    fn initialize(&self, p: InitPermit<'_>, value: u64) -> Result<(), Error> {
        check(
            p.as_ptr(),
            p.len(),
            size_of::<AtomicU64>(),
            align_of::<AtomicU64>(),
        )?;
        // SAFETY: 初始化许可独占，地址满足 AtomicU64 对齐和尺寸，尚无活跃原子对象。
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
        // SAFETY: 已初始化原子单元且许可保证存活；不进行普通字节读取。
        Ok(unsafe { p.as_ptr().cast::<AtomicU64>().as_ref() }.load(Ordering::SeqCst))
    }
    fn update<'a>(&'a self, p: UpdatePermit<'a>) -> Result<&'a AtomicU64, Error> {
        check(
            p.as_ptr(),
            p.len(),
            size_of::<AtomicU64>(),
            align_of::<AtomicU64>(),
        )?;
        // SAFETY: 已初始化原子单元，返回引用限制在许可寿命，原子更新允许共享。
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
        // SAFETY: 稳定许可保证原子对象存活；仅读取逻辑数值。
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
        // SAFETY: 最后所有者的销毁许可，无并发访问且已初始化，仅销毁一次。
        unsafe { std::ptr::drop_in_place(p.as_ptr().cast::<AtomicU64>().as_ptr()) };
        Ok(())
    }
}
