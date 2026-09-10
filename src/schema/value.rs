//! 值的专家扩展契约。许可不可由安全外部代码构造，不直接暴露共享可变引用。
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
        {
            return Err(Error::Codec("值尺寸或对齐不满足槽容量"));
        }
        Ok(self)
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
permit!(InitPermit, "未发布记录的独占初始化许可。");
permit!(ReadPermit, "带存活保护的短期读取许可，不代表独占修改。");
permit!(UpdatePermit, "已通过记录写入仲裁的短期更新许可。");
permit!(StablePermit, "已排除并发更新、可生成磁盘映像的许可。");
permit!(DropPermit, "最后访问和设备引用已结束后的独占销毁许可。");

/// 活跃值表示与磁盘格式的转换接口。
///
/// # Safety
/// 实现必须校验尺寸与对齐，只在许可范围内访问；返回视图不得超过许可寿命。
/// 并发原子访问不得混用普通字节读取；初始化失败须可清理，销毁不得重复。
/// 容量不足或失败不得留下共享半值；磁盘字节不能恢复成进程指针或锁状态。
pub unsafe trait ValueLayout: Send + Sync + 'static {
    type Owned: Send + Sync + 'static;
    type Read<'a>
    where
        Self: 'a;
    type Update<'a>
    where
        Self: 'a;
    fn format_id(&self) -> FormatId;
    fn plan(&self, value: &Self::Owned) -> Result<ValuePlan, Error>;
    fn initialize(&self, permit: InitPermit<'_>, value: Self::Owned) -> Result<(), Error>;
    fn read<'a>(&'a self, permit: ReadPermit<'a>) -> Result<Self::Read<'a>, Error>;
    fn update<'a>(&'a self, permit: UpdatePermit<'a>) -> Result<Self::Update<'a>, Error>;
    fn encode_stable(&self, permit: StablePermit<'_>, output: &mut [u8]) -> Result<(), Error>;
    fn decode_initialize(&self, encoded: &[u8], permit: InitPermit<'_>) -> Result<(), Error>;
    fn drop_value(&self, permit: DropPermit<'_>) -> Result<(), Error>;
}

/// 视图借用不能通过返回引用逃逸到拥有型输出。
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
