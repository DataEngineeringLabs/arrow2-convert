use std::borrow::Borrow;

use crate::field::{
    ArrowEnableVecForType, ArrowField, FixedSizeBinary, FixedSizeList, GenericBinary, GenericList,
    GenericUtf8, I128,
};

use super::{array_to_collection, ArrayAdapter, ArrowDeserialize, Nullable};
use arrow2::array::*;
use chrono::{NaiveDate, NaiveDateTime};

// Macro to facilitate implementation for numeric types and numeric arrays.
macro_rules! impl_arrow_deserialize_primitive {
    ($t:ty) => {
        impl ArrowDeserialize for $t {
            type Array = PrimitiveArray<$t>;

            #[inline]
            fn arrow_deserialize<'a>(v: &$t) -> Self {
                *v
            }
        }
    };
}

// blanket implementation for optional fields
impl<T> ArrowDeserialize for Option<T>
where
    T: ArrowDeserialize,
    for<'a> Nullable<<T as ArrowDeserialize>::Array>: ArrayAdapter<
        Element<'a> = Option<<<T as ArrowDeserialize>::Array as ArrayAdapter>::Element<'a>>,
    >,
{
    type Array = Nullable<<T as ArrowDeserialize>::Array>;

    #[inline]
    fn arrow_deserialize(
        v: Option<<<T as ArrowDeserialize>::Array as ArrayAdapter>::Element<'_>>,
    ) -> Option<<T as ArrowField>::Type> {
        v.map(T::arrow_deserialize)
    }
}

impl_arrow_deserialize_primitive!(u8);
impl_arrow_deserialize_primitive!(u16);
impl_arrow_deserialize_primitive!(u32);
impl_arrow_deserialize_primitive!(u64);
impl_arrow_deserialize_primitive!(i8);
impl_arrow_deserialize_primitive!(i16);
impl_arrow_deserialize_primitive!(i32);
impl_arrow_deserialize_primitive!(i64);
impl_arrow_deserialize_primitive!(f32);
impl_arrow_deserialize_primitive!(f64);

impl<const PRECISION: usize, const SCALE: usize> ArrowDeserialize for I128<PRECISION, SCALE> {
    type Array = PrimitiveArray<i128>;

    #[inline]
    fn arrow_deserialize(v: &i128) -> i128 {
        *v
    }
}

impl ArrowDeserialize for String {
    type Array = Utf8Array<i32>;

    #[inline]
    fn arrow_deserialize(v: &str) -> String {
        v.into()
    }
}

impl<S> ArrowDeserialize for GenericUtf8<i32, S>
where
    for<'a> S: From<&'a str>,
{
    type Array = Utf8Array<i32>;

    #[inline]
    fn arrow_deserialize(v: &str) -> S {
        v.into()
    }
}

impl<S> ArrowDeserialize for GenericUtf8<i64, S>
where
    for<'a> S: From<&'a str>,
{
    type Array = Utf8Array<i64>;

    #[inline]
    fn arrow_deserialize(v: &str) -> S {
        v.into()
    }
}

impl ArrowDeserialize for bool {
    type Array = BooleanArray;

    #[inline]
    fn arrow_deserialize(v: bool) -> Self {
        v
    }
}

impl ArrowDeserialize for NaiveDateTime {
    type Array = PrimitiveArray<i64>;

    #[inline]
    fn arrow_deserialize(v: &i64) -> Self {
        arrow2::temporal_conversions::timestamp_ns_to_datetime(*v)
    }
}

impl ArrowDeserialize for NaiveDate {
    type Array = PrimitiveArray<i32>;

    #[inline]
    fn arrow_deserialize(v: &i32) -> Self {
        arrow2::temporal_conversions::date32_to_date(*v)
    }
}

impl ArrowDeserialize for Vec<u8> {
    type Array = BinaryArray<i32>;

    #[inline]
    fn arrow_deserialize(v: &[u8]) -> Vec<u8> {
        v.iter().map(|v| *v).collect()
    }
}

impl<'a, C> ArrowDeserialize for GenericBinary<i32, C>
where
    Self: 'a,
    C: FromIterator<u8>,
    &'a C: IntoIterator<Item = &'a u8>,
{
    type Array = BinaryArray<i32>;

    #[inline]
    fn arrow_deserialize(v: &[u8]) -> C {
        v.iter().map(|v| *v).collect()
    }
}

impl<'a, C> ArrowDeserialize for GenericBinary<i64, C>
where
    Self: 'a,
    C: FromIterator<u8>,
    &'a C: IntoIterator<Item = &'a u8>,
{
    type Array = BinaryArray<i64>;

    #[inline]
    fn arrow_deserialize(v: &[u8]) -> C {
        v.iter().map(|v| *v).collect()
    }
}

impl<'a, const SIZE: usize, C> ArrowDeserialize for FixedSizeBinary<C, SIZE>
where
    Self: 'a,
    C: FromIterator<u8>,
    &'a C: IntoIterator<Item = &'a u8>,
{
    type Array = FixedSizeBinaryArray;

    #[inline]
    fn arrow_deserialize(v: &[u8]) -> C {
        v.iter().map(|v| *v).collect()
    }
}

// Blanket implementation for Vec
impl<T> ArrowDeserialize for Vec<T>
where
    T: ArrowField<Type = T> + ArrowDeserialize + ArrowEnableVecForType + 'static,
{
    type Array = ListArray<i32>;

    fn arrow_deserialize(array: Box<dyn arrow2::array::Array>) -> <Self as ArrowField>::Type {
        array_to_collection::<T, Self>(array.borrow())
    }
}

impl<'a, T, C> ArrowDeserialize for GenericList<i32, C, T>
where
    T: ArrowDeserialize + 'static,
    &'a C: IntoIterator<Item = &'a <T as ArrowField>::Type>,
    C: FromIterator<<T as ArrowField>::Type> + 'static + Default,
{
    type Array = ListArray<i32>;

    fn arrow_deserialize(array: Box<dyn arrow2::array::Array>) -> <Self as ArrowField>::Type {
        array_to_collection::<T, C>(array.borrow())
    }
}

impl<'a, T, C> ArrowDeserialize for GenericList<i64, C, T>
where
    T: ArrowDeserialize + 'static,
    &'a C: IntoIterator<Item = &'a <T as ArrowField>::Type>,
    C: FromIterator<<T as ArrowField>::Type> + 'static + Default,
{
    type Array = ListArray<i64>;

    fn arrow_deserialize(array: Box<dyn arrow2::array::Array>) -> <Self as ArrowField>::Type {
        array_to_collection::<T, C>(array.borrow())
    }
}

impl<'a, T, C, const SIZE: usize> ArrowDeserialize for FixedSizeList<C, T, SIZE>
where
    T: ArrowDeserialize + 'static,
    &'a C: IntoIterator<Item = &'a <T as ArrowField>::Type>,
    C: FromIterator<<T as ArrowField>::Type> + 'static + Default,
{
    type Array = FixedSizeListArray;

    fn arrow_deserialize(array: Box<dyn arrow2::array::Array>) -> <Self as ArrowField>::Type {
        array_to_collection::<T, C>(array.borrow())
    }
}
