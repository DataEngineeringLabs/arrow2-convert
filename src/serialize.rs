
use arrow2::array::*;
use chrono::{NaiveDate,NaiveDateTime};
use std::sync::Arc;

use crate::*;

/// Trait that is implemented by all types that are serializable to Arrow.
/// 
/// Implementations are provided for all built-in arrow types as well as Vec<T>, [T], Option<T>
/// if T implements ArrowSerialize. 
/// 
/// Note that Vec<T> implementation needs to be enabled by the [`crate::arrow_enable_vec_for_type`] macro.
/// 
/// Design notes:
/// 
/// The [`ArrowSerialize::SerializeOutput`] need to be manually specified for now, unless there's
/// a way to specify bounds on the output of `serialize`. Alternately, if generic associated types
/// were available, it could be specified as part of the [`ArrowMutableArray`] implementation.
pub trait ArrowSerialize: ArrowField + Sized
    where Self::MutableArrayType: ArrowMutableArray
        + arrow2::array::TryPush<Option<Self::SerializeOutput>>
        + arrow2::array::TryExtend<Option<Self::SerializeOutput>>
{
    /// The [`arrow2::array::MutableArray`] that holds this value
    type MutableArrayType;
    /// The output of [`serialize`] that the mutable array can accept
    type SerializeOutput;

    // serialize to arrow
    fn arrow_serialize(v: Option<Self>) -> Option<Self::SerializeOutput>;  

    // Internal
    // A hack for consistency with deserialize so that users can consistently implement
    // serialize/deserialize that take Option as input and output
    #[inline]
    fn arrow_serialize_internal(v: Self) -> Option<Self::SerializeOutput> {
        Self::arrow_serialize(Some(v))
    }
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
    // Convert the array into the arc type
    fn into_arc(self) -> Arc<dyn Array>;
}

pub trait ArrowMutableArrayTryPushGeneric<T>: ArrowMutableArray
where T: ArrowSerialize,
    Self: arrow2::array::TryPush<Option<T::SerializeOutput>>,
{
    #[inline]
    fn try_push_generic(&mut self, t: T) -> arrow2::error::Result<()> {
        <Self as arrow2::array::TryPush<Option<T::SerializeOutput>>>::try_push(self, <T as ArrowSerialize>::arrow_serialize_internal(t))
    }

    #[inline]
    fn try_extend_generic<I>(&mut self, iter: I) -> arrow2::error::Result<()> 
    where I: Iterator<Item = T>
    {
        for i in iter {
            <Self as ArrowMutableArrayTryPushGeneric<T>>::try_push_generic(self, i)?;
        }
        Ok(())
    }
}

// Macro to facilitate implementation for numeric types and numeric mutable arrays.
macro_rules! impl_numeric_type {
    ($physical_type:ty, $logical_type:ident) => {
        impl ArrowSerialize for $physical_type {
            type MutableArrayType = MutablePrimitiveArray<$physical_type>;
            type SerializeOutput = $physical_type;

            #[inline]
            fn arrow_serialize(v: Option<Self>) -> Option<$physical_type> {
                v
            }
        }

        impl ArrowMutableArray for MutablePrimitiveArray<$physical_type> {
            impl_mutable_array_body!();
        }

        impl<T> ArrowMutableArrayTryPushGeneric<T> for MutablePrimitiveArray<$physical_type>
        where T: ArrowSerialize, Self: arrow2::array::TryPush<Option<T::SerializeOutput>>
        {}
    };
}

// Macro to facilitate implementing ArrowMutableArray
macro_rules! impl_mutable_array_body {
    () => {
        #[inline]
        fn into_arc(self) -> Arc<dyn Array> {
            Self::into_arc(self)
        }
    };
}

// blanket implementation for optional fields
impl<T> ArrowSerialize for Option<T>
where T: ArrowSerialize
{
    type MutableArrayType = <T as ArrowSerialize>::MutableArrayType;
    type SerializeOutput = <T as ArrowSerialize>::SerializeOutput;

    #[inline]
    fn arrow_serialize_internal(v: Self) -> Option<<T as ArrowSerialize>::SerializeOutput> {
        <T as ArrowSerialize>::arrow_serialize(v)
    }

    #[inline]
    fn arrow_serialize(v: Option<Self>) -> Option<<T as ArrowSerialize>::SerializeOutput> {
        match v {
            Some(t) => Self::arrow_serialize_internal(t),
            None => None
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
    type SerializeOutput = String;

    #[inline]
    fn arrow_serialize(v: Option<Self>) -> Option<String> {
        v
    }
}

impl ArrowSerialize for bool
{
    type MutableArrayType = MutableBooleanArray;
    type SerializeOutput = bool;

    #[inline]
    fn arrow_serialize(v: Option<Self>) -> Option<bool> {
        v
    }
}

impl ArrowSerialize for NaiveDateTime
{
    type MutableArrayType = MutablePrimitiveArray<i64>;
    type SerializeOutput = i64;

    #[inline]
    fn arrow_serialize(v: Option<Self>) -> Option<i64> {
        v.map(|t| t.timestamp_nanos())
    }
}

impl ArrowSerialize for NaiveDate
{
    type MutableArrayType = MutablePrimitiveArray<i32>;
    type SerializeOutput = i32;

    #[inline]
    fn arrow_serialize(v: Option<Self>) -> Option<i32> {
        v.map(|t| (chrono::Datelike::num_days_from_ce(&t) - arrow2::temporal_conversions::EPOCH_DAYS_FROM_CE))
    }
}

impl ArrowSerialize for Vec<u8> {
    type MutableArrayType = MutableBinaryArray<i32>;
    type SerializeOutput = Vec<u8>;

    #[inline]
    fn arrow_serialize(v: Option<Self>) -> Option<Vec<u8>> {
        v
    }
}

// Blanket implementation for Vec. 
impl<T> ArrowSerialize for Vec<T>
where T: ArrowSerialize + ArrowEnableVecForType + 'static,
    T::MutableArrayType: arrow2::array::TryPush<Option<T::SerializeOutput>>,
    T::MutableArrayType: arrow2::array::TryExtend<Option<T::SerializeOutput>>
{
    type MutableArrayType = MutableListArray<i32, <T as ArrowSerialize>::MutableArrayType>;
    type SerializeOutput = Vec<Option<T::SerializeOutput>>;

    fn arrow_serialize(_v: Option<Self>) -> Option<Self::SerializeOutput> {
        unimplemented!()
    }
}

impl ArrowMutableArray for MutableBooleanArray {
    impl_mutable_array_body!();
}

impl<T> ArrowMutableArrayTryPushGeneric<T> for MutableBooleanArray
where T: ArrowSerialize, Self: arrow2::array::TryPush<Option<T::SerializeOutput>>
{}

impl ArrowMutableArray for MutableUtf8Array<i32>
{
    impl_mutable_array_body!();
}

impl<T> ArrowMutableArrayTryPushGeneric<T> for MutableUtf8Array<i32>
where T: ArrowSerialize, 
    Self: arrow2::array::TryPush<Option<T::SerializeOutput>>
{}

impl ArrowMutableArray for MutableBinaryArray<i32>
{
    impl_mutable_array_body!();
}

impl<T> ArrowMutableArrayTryPushGeneric<T> for MutableBinaryArray<i32>
where T: ArrowSerialize, Self: arrow2::array::TryPush<Option<T::SerializeOutput>>
{}

impl<M> ArrowMutableArray for MutableListArray<i32, M>
    where M: ArrowMutableArray + 'static
{
    impl_mutable_array_body!();
}

impl<M, T> ArrowMutableArrayTryPushGeneric<Vec<T>> for MutableListArray<i32, M>
where M: ArrowMutableArray 
        + ArrowMutableArrayTryPushGeneric<T> 
        + arrow2::array::TryPush<Option<T::SerializeOutput>> 
        + arrow2::array::TryExtend<Option<T::SerializeOutput>> 
        + 'static,
    T: ArrowSerialize + ArrowEnableVecForType + 'static,
{
    fn try_push_generic(&mut self, t: Vec<T>) -> arrow2::error::Result<()>
    {
        let mut values = self.mut_values();
        for i in t.into_iter() {
            <M as ArrowMutableArrayTryPushGeneric<T>>::try_push_generic(&mut values, i)?;
        }
        self.try_push_valid()?;
        Ok(())
    }
}

impl<M, T> ArrowMutableArrayTryPushGeneric<Option<Vec<T>>> for MutableListArray<i32, M>
where M: ArrowMutableArray 
        + ArrowMutableArrayTryPushGeneric<T> 
        + arrow2::array::TryPush<Option<T::SerializeOutput>> 
        + arrow2::array::TryExtend<Option<T::SerializeOutput>> 
        + 'static,
    T: ArrowSerialize + ArrowEnableVecForType + 'static,
{
    fn try_push_generic(&mut self, t: Option<Vec<T>>) -> arrow2::error::Result<()>
    {
        match t {
            Some(v) => {
                <Self as ArrowMutableArrayTryPushGeneric<Vec<T>>>::try_push_generic(self, v)?;
            },
            None => {
                <Self as MutableArray>::push_null(self);
            }
        }
        Ok(())
    }
}
