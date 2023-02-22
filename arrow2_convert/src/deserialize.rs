//! Implementation and traits for deserializing from Arrow.

use arrow2::{array::*, buffer::Buffer, types::NativeType};
use chrono::{NaiveDate, NaiveDateTime};

use crate::field::*;

/// Implemented by [`ArrowField`] that can be deserialized from arrow
pub trait ArrowDeserialize: ArrowField + Sized
where
    Self::ArrayType: ArrowArray,
    for<'a> &'a Self::ArrayType: IntoIterator,
{
    /// The `arrow2::Array` type corresponding to this field
    type ArrayType;

    /// Deserialize this field from arrow
    fn arrow_deserialize(
        v: <&Self::ArrayType as IntoIterator>::Item,
    ) -> Option<<Self as ArrowField>::Type>;

    #[inline]
    #[doc(hidden)]
    /// For internal use only
    ///
    /// This is an ugly hack to allow generating a blanket Option<T> deserialize.
    /// Ideally we would be able to capture the optional field of the iterator via
    /// something like for<'a> &'a T::ArrayType: IntoIterator<Item=Option<E>>,
    /// However, the E parameter seems to confuse the borrow checker if it's a reference.
    fn arrow_deserialize_internal(
        v: <&Self::ArrayType as IntoIterator>::Item,
    ) -> <Self as ArrowField>::Type {
        Self::arrow_deserialize(v).unwrap()
    }
}

/// Internal trait used to support deserialization and iteration of structs, and nested struct lists
///
/// Trivial pass-thru implementations are provided for arrow2 arrays that implement IntoIterator.
///
/// The derive macro generates implementations for typed struct arrays.
#[doc(hidden)]
pub trait ArrowArray
where
    for<'a> &'a Self: IntoIterator,
{
    type BaseArrayType: Array;

    // Returns a typed iterator to the underlying elements of the array from an untyped Array reference.
    fn iter_from_array_ref(b: &dyn Array) -> <&Self as IntoIterator>::IntoIter;
}

// Macro to facilitate implementation for numeric types and numeric arrays.
macro_rules! impl_arrow_deserialize_primitive {
    ($physical_type:ty) => {
        impl ArrowDeserialize for $physical_type {
            type ArrayType = PrimitiveArray<$physical_type>;

            #[inline]
            fn arrow_deserialize<'a>(v: Option<&$physical_type>) -> Option<Self> {
                v.map(|t| *t)
            }
        }

        impl_arrow_array!(PrimitiveArray<$physical_type>);
    };
}

macro_rules! impl_arrow_array {
    ($array:ty) => {
        impl ArrowArray for $array {
            type BaseArrayType = Self;

            #[inline]
            fn iter_from_array_ref(b: &dyn Array) -> <&Self as IntoIterator>::IntoIter {
                b.as_any()
                    .downcast_ref::<Self::BaseArrayType>()
                    .unwrap()
                    .into_iter()
            }
        }
    };
}

// blanket implementation for optional fields
impl<T> ArrowDeserialize for Option<T>
where
    T: ArrowDeserialize,
    T::ArrayType: 'static + ArrowArray,
    for<'a> &'a T::ArrayType: IntoIterator,
{
    type ArrayType = <T as ArrowDeserialize>::ArrayType;

    #[inline]
    fn arrow_deserialize(
        v: <&Self::ArrayType as IntoIterator>::Item,
    ) -> Option<<Self as ArrowField>::Type> {
        Self::arrow_deserialize_internal(v).map(Some)
    }

    #[inline]
    fn arrow_deserialize_internal(
        v: <&Self::ArrayType as IntoIterator>::Item,
    ) -> <Self as ArrowField>::Type {
        <T as ArrowDeserialize>::arrow_deserialize(v)
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
    type ArrayType = PrimitiveArray<i128>;

    #[inline]
    fn arrow_deserialize<'a>(v: Option<&i128>) -> Option<i128> {
        v.copied()
    }
}

impl_arrow_array!(PrimitiveArray<i128>);

impl ArrowDeserialize for String {
    type ArrayType = Utf8Array<i32>;

    #[inline]
    fn arrow_deserialize(v: Option<&str>) -> Option<Self> {
        v.map(|t| t.to_string())
    }
}

impl ArrowDeserialize for LargeString {
    type ArrayType = Utf8Array<i64>;

    #[inline]
    fn arrow_deserialize(v: Option<&str>) -> Option<String> {
        v.map(|t| t.to_string())
    }
}

impl ArrowDeserialize for bool {
    type ArrayType = BooleanArray;

    #[inline]
    fn arrow_deserialize(v: Option<bool>) -> Option<Self> {
        v
    }
}

impl ArrowDeserialize for NaiveDateTime {
    type ArrayType = PrimitiveArray<i64>;

    #[inline]
    fn arrow_deserialize(v: Option<&i64>) -> Option<Self> {
        v.map(|t| arrow2::temporal_conversions::timestamp_ns_to_datetime(*t))
    }
}

impl ArrowDeserialize for NaiveDate {
    type ArrayType = PrimitiveArray<i32>;

    #[inline]
    fn arrow_deserialize(v: Option<&i32>) -> Option<Self> {
        v.map(|t| arrow2::temporal_conversions::date32_to_date(*t))
    }
}

/// Iterator for for [`BufferBinaryArray`]
pub struct BufferBinaryArrayIter<'a> {
    index: usize,
    array: &'a BinaryArray<i32>,
}

impl<'a> Iterator for BufferBinaryArrayIter<'a> {
    type Item = Option<Buffer<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            None
        } else {
            if let Some(validity) = self.array.validity() {
                if !validity.get_bit(self.index) {
                    self.index += 1;
                    return Some(None);
                }
            }
            let (start,end) = self.array.offsets().start_end(self.index);
            self.index += 1;
            Some(Some(self.array.values().clone().slice(start,end)))
        }
    }
}

/// Internal `ArrowArray` helper to iterate over a `BinaryArray` while exposing Buffer slices
pub struct BufferBinaryArray;

impl<'a> IntoIterator for &'a BufferBinaryArray {
    type Item = Option<Buffer<u8>>;

    type IntoIter = BufferBinaryArrayIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        unimplemented!("Use iter_from_array_ref");
    }
}

impl ArrowArray for BufferBinaryArray {
    type BaseArrayType = BinaryArray<i32>;
    #[inline]
    fn iter_from_array_ref(a: &dyn Array) -> <&Self as IntoIterator>::IntoIter {
        let b = a.as_any()
        .downcast_ref::<Self::BaseArrayType>()
        .unwrap();

        BufferBinaryArrayIter{
            index: 0,
            array: b
        }
    }
}

impl ArrowDeserialize for Buffer<u8> {
    type ArrayType = BufferBinaryArray;

    #[inline]
    fn arrow_deserialize(v: Option<Buffer<u8>>) -> Option<Self> {
        v
    }
}

impl ArrowDeserialize for Vec<u8> {
    type ArrayType = BinaryArray<i32>;

    #[inline]
    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Self> {
        v.map(|t| t.to_vec())
    }
}

impl ArrowDeserialize for LargeBinary {
    type ArrayType = BinaryArray<i64>;

    #[inline]
    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Vec<u8>> {
        v.map(|t| t.to_vec())
    }
}

impl<const SIZE: usize> ArrowDeserialize for FixedSizeBinary<SIZE> {
    type ArrayType = FixedSizeBinaryArray;

    #[inline]
    fn arrow_deserialize(v: Option<&[u8]>) -> Option<Vec<u8>> {
        v.map(|t| t.to_vec())
    }
}

fn arrow_deserialize_vec_helper<T>(
    v: Option<Box<dyn Array>>,
) -> Option<<Vec<T> as ArrowField>::Type>
where
    T: ArrowDeserialize + ArrowEnableVecForType + 'static,
    for<'a> &'a T::ArrayType: IntoIterator,
{
    use std::ops::Deref;
    v.map(|t| {
        arrow_array_deserialize_iterator_internal::<<T as ArrowField>::Type, T>(t.deref())
            .collect::<Vec<<T as ArrowField>::Type>>()
    })
}



// Blanket implementation for Buffer
impl<T> ArrowDeserialize for Buffer<T>
where
    T: ArrowDeserialize + NativeType + ArrowEnableVecForType,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoIterator,
{
    type ArrayType = ListArray<i32>;

    #[inline]
    fn arrow_deserialize(
        v: <&Self::ArrayType as IntoIterator>::Item,
    ) -> Option<<Self as ArrowField>::Type> {
        v.map(|t| {
            t.as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .unwrap()
                .values()
                .clone()
        })
    }
}

// Blanket implementation for Vec
impl<T> ArrowDeserialize for Vec<T>
where
    T: ArrowDeserialize + ArrowEnableVecForType + 'static,
    <T as ArrowDeserialize>::ArrayType: 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoIterator,
{
    type ArrayType = ListArray<i32>;

    fn arrow_deserialize(v: Option<Box<dyn Array>>) -> Option<<Self as ArrowField>::Type> {
        arrow_deserialize_vec_helper::<T>(v)
    }
}

impl<T> ArrowDeserialize for LargeVec<T>
where
    T: ArrowDeserialize + ArrowEnableVecForType + 'static,
    <T as ArrowDeserialize>::ArrayType: 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoIterator,
{
    type ArrayType = ListArray<i64>;

    fn arrow_deserialize(v: Option<Box<dyn Array>>) -> Option<<Self as ArrowField>::Type> {
        arrow_deserialize_vec_helper::<T>(v)
    }
}

impl<T, const SIZE: usize> ArrowDeserialize for FixedSizeVec<T, SIZE>
where
    T: ArrowDeserialize + ArrowEnableVecForType + 'static,
    <T as ArrowDeserialize>::ArrayType: 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoIterator,
{
    type ArrayType = FixedSizeListArray;

    fn arrow_deserialize(v: Option<Box<dyn Array>>) -> Option<<Self as ArrowField>::Type> {
        arrow_deserialize_vec_helper::<T>(v)
    }
}



impl_arrow_array!(BooleanArray);
impl_arrow_array!(Utf8Array<i32>);
impl_arrow_array!(Utf8Array<i64>);
impl_arrow_array!(BinaryArray<i32>);
impl_arrow_array!(BinaryArray<i64>);
impl_arrow_array!(FixedSizeBinaryArray);
impl_arrow_array!(ListArray<i32>);
impl_arrow_array!(ListArray<i64>);
impl_arrow_array!(FixedSizeListArray);

/// Top-level API to deserialize from Arrow
pub trait TryIntoCollection<Collection, Element>
where
    Element: ArrowField,
    Collection: FromIterator<Element>,
{
    /// Convert from a `arrow2::Array` to any collection that implements the `FromIterator` trait
    fn try_into_collection(self) -> arrow2::error::Result<Collection>;

    /// Same as `try_into_collection` except can coerce the conversion to a specific Arrow type. This is
    /// useful when the same rust type maps to one or more Arrow types for example `LargeString`.
    fn try_into_collection_as_type<ArrowType>(self) -> arrow2::error::Result<Collection>
    where
        ArrowType: ArrowDeserialize + ArrowField<Type = Element> + 'static,
        for<'b> &'b <ArrowType as ArrowDeserialize>::ArrayType: IntoIterator;
}

/// Helper to return an iterator for elements from a [`arrow2::array::Array`].
fn arrow_array_deserialize_iterator_internal<'a, Element, Field>(
    b: &'a dyn arrow2::array::Array,
) -> impl Iterator<Item = Element> + 'a
where
    Field: ArrowDeserialize + ArrowField<Type = Element> + 'static,
    for<'b> &'b <Field as ArrowDeserialize>::ArrayType: IntoIterator,
{
    <<Field as ArrowDeserialize>::ArrayType as ArrowArray>::iter_from_array_ref(b)
        .map(<Field as ArrowDeserialize>::arrow_deserialize_internal)
}

/// Returns a typed iterator to a target type from an `arrow2::Array`
pub fn arrow_array_deserialize_iterator_as_type<'a, Element, ArrowType>(
    arr: &'a dyn arrow2::array::Array,
) -> arrow2::error::Result<impl Iterator<Item = Element> + 'a>
where
    Element: 'static,
    ArrowType: ArrowDeserialize + ArrowField<Type = Element> + 'static,
    for<'b> &'b <ArrowType as ArrowDeserialize>::ArrayType: IntoIterator,
{
    if &<ArrowType as ArrowField>::data_type() != arr.data_type() {
        Err(arrow2::error::Error::InvalidArgumentError(
            "Data type mismatch".to_string(),
        ))
    } else {
        Ok(arrow_array_deserialize_iterator_internal::<
            Element,
            ArrowType,
        >(arr))
    }
}

/// Return an iterator that deserializes an [`Array`] to an element of type T
pub fn arrow_array_deserialize_iterator<'a, T>(
    arr: &'a dyn arrow2::array::Array,
) -> arrow2::error::Result<impl Iterator<Item = T> + 'a>
where
    T: ArrowDeserialize + ArrowField<Type = T> + 'static,
    for<'b> &'b <T as ArrowDeserialize>::ArrayType: IntoIterator,
{
    arrow_array_deserialize_iterator_as_type::<T, T>(arr)
}

impl<Collection, Element, ArrowArray> TryIntoCollection<Collection, Element> for ArrowArray
where
    Element: ArrowDeserialize + ArrowField<Type = Element> + 'static,
    for<'b> &'b <Element as ArrowDeserialize>::ArrayType: IntoIterator,
    ArrowArray: std::borrow::Borrow<dyn Array>,
    Collection: FromIterator<Element>,
{
    fn try_into_collection(self) -> arrow2::error::Result<Collection> {
        Ok(arrow_array_deserialize_iterator::<Element>(self.borrow())?.collect())
    }

    fn try_into_collection_as_type<ArrowType>(self) -> arrow2::error::Result<Collection>
    where
        ArrowType: ArrowDeserialize + ArrowField<Type = Element> + 'static,
        for<'b> &'b <ArrowType as ArrowDeserialize>::ArrayType: IntoIterator,
    {
        Ok(
            arrow_array_deserialize_iterator_as_type::<Element, ArrowType>(self.borrow())?
                .collect(),
        )
    }
}
