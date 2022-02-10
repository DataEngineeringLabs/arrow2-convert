
use arrow2::array::*;
use chrono::{NaiveDate,NaiveDateTime};
use std::sync::Arc;

use crate::field::{ArrowField,ArrowEnableVecForType};

/// Trait that is implemented by all types that are serializable to Arrow.
/// 
/// Implementations are provided for all built-in arrow types as well as Vec<T>, and Option<T>
/// if T implements ArrowSerialize. 
/// 
/// Note that Vec<T> implementation needs to be enabled by the [`crate::arrow_enable_vec_for_type`] macro.
pub trait ArrowSerialize: ArrowField
{
    /// The [`arrow2::array::MutableArray`] that holds this value
    type MutableArrayType: ArrowMutableArray;
 
    /// Serialize this field to arrow
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()>;
}

/// This trait provides an interface that's exposed by all Mutable lists but that are not 
/// part of the official MutableArray API. 
/// 
/// Implementations of this trait are provided for all mutable arrays provided by
/// [`arrow2`].
#[doc(hidden)]
pub trait ArrowMutableArray:
    arrow2::array::MutableArray
    + Default
{
    fn reserve(&mut self, additional: usize, additional_values: usize);
}

// Macro to facilitate implementation of serializable traits for numeric types and numeric mutable arrays.
macro_rules! impl_numeric_type {
    ($physical_type:ty, $logical_type:ident) => {
        impl ArrowSerialize for $physical_type {
            type MutableArrayType = MutablePrimitiveArray<$physical_type>;

            #[inline]
            fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
                array.try_push(Some(*v))
            }
        }

        impl ArrowMutableArray for MutablePrimitiveArray<$physical_type> {
            impl_mutable_array_body!();
        }
    };
}

// Macro to facilitate implementing ArrowMutableArray
macro_rules! impl_mutable_array_body {
    () => {
        #[inline]
        fn reserve(&mut self, additional: usize, _additional_values: usize) {
            self.reserve(additional);
        }
    };
}

// blanket implementation for optional fields
impl<T> ArrowSerialize for Option<T>
where T: ArrowSerialize
{
    type MutableArrayType = <T as ArrowSerialize>::MutableArrayType;

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        match v.as_ref() {
            Some(t) => { <T as ArrowSerialize>::arrow_serialize(t, array) },
            None => { array.push_null(); Ok(()) },
        }
    }
}

impl_numeric_type!(u8, UInt8);
impl_numeric_type!(u16, UInt16);
impl_numeric_type!(u32, UInt32);
impl_numeric_type!(u64, UInt64);
impl_numeric_type!(i8, Int8);
impl_numeric_type!(i16, Int16);
impl_numeric_type!(i32, Int32);
impl_numeric_type!(i64, Int64);
impl_numeric_type!(f32, Float32);
impl_numeric_type!(f64, Float64);

impl ArrowSerialize for String
{
    type MutableArrayType = MutableUtf8Array<i32>;

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(v))
    }
}

impl ArrowSerialize for str
{
    type MutableArrayType = MutableUtf8Array<i32>;

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(v))
    }
}

impl ArrowSerialize for bool
{
    type MutableArrayType = MutableBooleanArray;

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(*v))
    }
}

impl ArrowSerialize for NaiveDateTime
{
    type MutableArrayType = MutablePrimitiveArray<i64>;

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(v.timestamp_nanos()))
    }
}

impl ArrowSerialize for NaiveDate
{
    type MutableArrayType = MutablePrimitiveArray<i32>;

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(chrono::Datelike::num_days_from_ce(v) - arrow2::temporal_conversions::EPOCH_DAYS_FROM_CE))
    }
}

impl ArrowSerialize for Vec<u8> {
    type MutableArrayType = MutableBinaryArray<i32>;

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(v))
    }
}

impl ArrowSerialize for [u8] {
    type MutableArrayType = MutableBinaryArray<i32>;

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(v))
    }
}

// Blanket implementation for Vec
impl<T> ArrowSerialize for Vec<T>
where T: ArrowSerialize + ArrowEnableVecForType + 'static
{
    type MutableArrayType = MutableListArray<i32, <T as ArrowSerialize>::MutableArrayType>;

    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        let values = array.mut_values();
        for i in v.iter() {
            <T as ArrowSerialize>::arrow_serialize(i, values)?;
        }
        array.try_push_valid()
    }
}

// Blanket implementation for [T]
impl<T> ArrowSerialize for [T]
where T: ArrowSerialize + ArrowEnableVecForType + 'static
{
    type MutableArrayType = MutableListArray<i32, <T as ArrowSerialize>::MutableArrayType>;

    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        let values = array.mut_values();
        for i in v.iter() {
            <T as ArrowSerialize>::arrow_serialize(i, values)?;
        }
        array.try_push_valid()
    }
}

impl ArrowMutableArray for MutableBooleanArray {
    impl_mutable_array_body!();
}

impl ArrowMutableArray for MutableUtf8Array<i32>
{
    #[inline]
    fn reserve(&mut self, additional: usize, additional_values: usize) {
        self.reserve(additional, additional_values);
    }
}

impl ArrowMutableArray for MutableBinaryArray<i32>
{
    impl_mutable_array_body!();
}

impl<M> ArrowMutableArray for MutableListArray<i32, M>
    where M: ArrowMutableArray + 'static
{
    #[inline]
    fn reserve(&mut self, _additional: usize, _additional_values: usize) {}
}

// internal helper method to extend and serialize a mutable array
fn arrow_serialize_and_extend<'a, T: ArrowSerialize + 'static, I: IntoIterator<Item = &'a T>>(
    into_iter: I, array: &mut <T as ArrowSerialize>::MutableArrayType) -> arrow2::error::Result<()>
{
    let iter = into_iter.into_iter();
    array.reserve(iter.size_hint().0, 0);
    for i in iter {
        <T as ArrowSerialize>::arrow_serialize(i, array)?;
    }
    Ok(())
}

/// Top-level API to serialize to Arrow
pub trait IntoArrow<T>
{
    fn into_arrow(self) -> arrow2::error::Result<T>;
}

impl<'a, T, I> IntoArrow<Arc<dyn Array>> for I
where T: ArrowSerialize + 'static,
    I: IntoIterator<Item = &'a T>
{
    fn into_arrow(self) -> arrow2::error::Result<Arc<dyn Array>> {
        let mut arr = <T as ArrowSerialize>::MutableArrayType::default();
        arrow_serialize_and_extend(self, &mut arr)?;
        Ok(arr.as_arc())
    }
}

impl<'a, T, I> IntoArrow<Box<dyn Array>> for I
where T: ArrowSerialize + 'static,
    I: IntoIterator<Item = &'a T>
{
    fn into_arrow(self) -> arrow2::error::Result<Box<dyn Array>> {
        let mut arr = <T as ArrowSerialize>::MutableArrayType::default();
        arrow_serialize_and_extend(self, &mut arr)?;
        Ok(arr.as_box())
    }
}
