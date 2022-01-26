// The proc macro is implemented in derive_internal, and re-exported by this
// crate. This is because a single crate can not define both a proc macro and a
// macro_rules macro.
pub use derive_internal::ArrowStruct;

mod deserialize;
mod field;
mod serialize;

pub use deserialize::{ArrowDeserialize,ArrowArray};
pub use field::{ArrowEnableVecForType, ArrowField};
pub use serialize::{ArrowSerialize,ArrowMutableArray,ArrowMutableArrayTryPushGeneric};

pub fn arrow_serialize<T>(t: Vec<T>) -> arrow2::error::Result<std::sync::Arc<dyn arrow2::array::Array>>
where T: ArrowSerialize,
    <T as ArrowSerialize>::MutableArrayType: ArrowMutableArrayTryPushGeneric<T>
{
    let mut arr = <T as ArrowSerialize>::MutableArrayType::default();
    <<T as ArrowSerialize>::MutableArrayType as ArrowMutableArrayTryPushGeneric<T>>::try_extend_generic(&mut arr, t.into_iter())?;
    Ok(arr.into_arc())
}

pub fn arrow_deserialize<'a, T>(b: &'a dyn arrow2::array::Array) -> arrow2::error::Result<Vec<T>>
where T: ArrowDeserialize + 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoIterator
{
    Ok(deserialize::arrow_array_typed_iterator(b)?.collect())
}
