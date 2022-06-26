use arrow2::array::*;
use arrow2::chunk::Chunk;
use chrono::{NaiveDate, NaiveDateTime};
use std::sync::Arc;

use crate::field::*;

/// Trait that is implemented by all types that are serializable to Arrow.
///
/// Implementations are provided for all built-in arrow types as well as Vec<T>, and Option<T>
/// if T implements ArrowSerialize.
///
/// Note that Vec<T> implementation needs to be enabled by the [`crate::arrow_enable_vec_for_type`] macro.
pub trait ArrowSerialize: ArrowField {
    /// The [`arrow2::array::MutableArray`] that holds this value
    type MutableArrayType: ArrowMutableArray;

    /// Create a new mutable array
    fn new_array() -> Self::MutableArrayType;

    /// Serialize this field to arrow
    fn arrow_serialize(
        v: &<Self as ArrowField>::Type,
        array: &mut Self::MutableArrayType,
    ) -> arrow2::error::Result<()>;
}

/// This trait provides an interface that's exposed by all Mutable lists but that are not
/// part of the official MutableArray API.
///
/// Implementations of this trait are provided for all mutable arrays provided by [`arrow2`].
#[doc(hidden)]
pub trait ArrowMutableArray: arrow2::array::MutableArray {
    fn reserve(&mut self, additional: usize, additional_values: usize);
}

// Macro to facilitate implementation of serializable traits for numeric types and numeric mutable arrays.
macro_rules! impl_numeric_type {
    ($physical_type:ty, $logical_type:ident) => {
        impl ArrowSerialize for $physical_type {
            type MutableArrayType = MutablePrimitiveArray<$physical_type>;

            #[inline]
            fn new_array() -> Self::MutableArrayType {
                Self::MutableArrayType::default()
            }

            #[inline]
            fn arrow_serialize(
                v: &Self,
                array: &mut Self::MutableArrayType,
            ) -> arrow2::error::Result<()> {
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
where
    T: ArrowSerialize,
{
    type MutableArrayType = <T as ArrowSerialize>::MutableArrayType;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        <T as ArrowSerialize>::new_array()
    }

    #[inline]
    fn arrow_serialize(
        v: &<Self as ArrowField>::Type,
        array: &mut Self::MutableArrayType,
    ) -> arrow2::error::Result<()> {
        match v.as_ref() {
            Some(t) => <T as ArrowSerialize>::arrow_serialize(t, array),
            None => {
                array.push_null();
                Ok(())
            }
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

impl ArrowSerialize for String {
    type MutableArrayType = MutableUtf8Array<i32>;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::default()
    }

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(v))
    }
}

impl ArrowSerialize for LargeString {
    type MutableArrayType = MutableUtf8Array<i64>;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::default()
    }

    #[inline]
    fn arrow_serialize(
        v: &String,
        array: &mut Self::MutableArrayType,
    ) -> arrow2::error::Result<()> {
        array.try_push(Some(v))
    }
}

impl ArrowSerialize for bool {
    type MutableArrayType = MutableBooleanArray;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::default()
    }

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(*v))
    }
}

impl ArrowSerialize for NaiveDateTime {
    type MutableArrayType = MutablePrimitiveArray<i64>;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::from(<Self as ArrowField>::data_type())
    }

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(v.timestamp_nanos()))
    }
}

impl ArrowSerialize for NaiveDate {
    type MutableArrayType = MutablePrimitiveArray<i32>;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::from(<Self as ArrowField>::data_type())
    }

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(
            chrono::Datelike::num_days_from_ce(v)
                - arrow2::temporal_conversions::EPOCH_DAYS_FROM_CE,
        ))
    }
}

impl ArrowSerialize for Vec<u8> {
    type MutableArrayType = MutableBinaryArray<i32>;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::default()
    }

    #[inline]
    fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
        array.try_push(Some(v))
    }
}

impl ArrowSerialize for LargeBinary {
    type MutableArrayType = MutableBinaryArray<i64>;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::default()
    }

    #[inline]
    fn arrow_serialize(
        v: &Vec<u8>,
        array: &mut Self::MutableArrayType,
    ) -> arrow2::error::Result<()> {
        array.try_push(Some(v))
    }
}

impl<const SIZE: usize> ArrowSerialize for FixedSizeBinary<SIZE> {
    type MutableArrayType = MutableFixedSizeBinaryArray;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::new(SIZE)
    }

    #[inline]
    fn arrow_serialize(
        v: &Vec<u8>,
        array: &mut Self::MutableArrayType,
    ) -> arrow2::error::Result<()> {
        array.try_push(Some(v))
    }
}

// Blanket implementation for Vec
impl<T> ArrowSerialize for Vec<T>
where
    T: ArrowSerialize + ArrowEnableVecForType + 'static,
    <T as ArrowSerialize>::MutableArrayType: Default,
{
    type MutableArrayType = MutableListArray<i32, <T as ArrowSerialize>::MutableArrayType>;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::new_with_field(
            <T as ArrowSerialize>::new_array(),
            "item",
            <T as ArrowField>::is_nullable(),
        )
    }

    fn arrow_serialize(
        v: &<Self as ArrowField>::Type,
        array: &mut Self::MutableArrayType,
    ) -> arrow2::error::Result<()> {
        let values = array.mut_values();
        for i in v.iter() {
            <T as ArrowSerialize>::arrow_serialize(i, values)?;
        }
        array.try_push_valid()
    }
}

impl<T> ArrowSerialize for LargeVec<T>
where
    T: ArrowSerialize + ArrowEnableVecForType + 'static,
    <T as ArrowSerialize>::MutableArrayType: Default,
{
    type MutableArrayType = MutableListArray<i64, <T as ArrowSerialize>::MutableArrayType>;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::new_with_field(
            <T as ArrowSerialize>::new_array(),
            "item",
            <T as ArrowField>::is_nullable(),
        )
    }

    fn arrow_serialize(
        v: &<Self as ArrowField>::Type,
        array: &mut Self::MutableArrayType,
    ) -> arrow2::error::Result<()> {
        let values = array.mut_values();
        for i in v.iter() {
            <T as ArrowSerialize>::arrow_serialize(i, values)?;
        }
        array.try_push_valid()
    }
}

impl<T, const SIZE: usize> ArrowSerialize for FixedSizeVec<T, SIZE>
where
    T: ArrowSerialize + ArrowEnableVecForType + 'static,
    <T as ArrowSerialize>::MutableArrayType: Default,
{
    type MutableArrayType = MutableFixedSizeListArray<<T as ArrowSerialize>::MutableArrayType>;

    #[inline]
    fn new_array() -> Self::MutableArrayType {
        Self::MutableArrayType::new_with_field(
            <T as ArrowSerialize>::new_array(),
            "item",
            <T as ArrowField>::is_nullable(),
            SIZE,
        )
    }

    fn arrow_serialize(
        v: &<Self as ArrowField>::Type,
        array: &mut Self::MutableArrayType,
    ) -> arrow2::error::Result<()> {
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

impl ArrowMutableArray for MutableUtf8Array<i32> {
    #[inline]
    fn reserve(&mut self, additional: usize, additional_values: usize) {
        self.reserve(additional, additional_values);
    }
}

impl ArrowMutableArray for MutableUtf8Array<i64> {
    #[inline]
    fn reserve(&mut self, additional: usize, additional_values: usize) {
        self.reserve(additional, additional_values);
    }
}

impl ArrowMutableArray for MutableBinaryArray<i32> {
    impl_mutable_array_body!();
}

impl ArrowMutableArray for MutableBinaryArray<i64> {
    impl_mutable_array_body!();
}

impl ArrowMutableArray for MutableFixedSizeBinaryArray {
    #[inline]
    fn reserve(&mut self, _additional: usize, _additional_values: usize) {}
}

impl<M> ArrowMutableArray for MutableListArray<i32, M>
where
    M: ArrowMutableArray + Default + 'static,
{
    #[inline]
    fn reserve(&mut self, _additional: usize, _additional_values: usize) {}
}

impl<M> ArrowMutableArray for MutableListArray<i64, M>
where
    M: ArrowMutableArray + Default + 'static,
{
    #[inline]
    fn reserve(&mut self, _additional: usize, _additional_values: usize) {}
}

impl<M> ArrowMutableArray for MutableFixedSizeListArray<M>
where
    M: ArrowMutableArray + Default + 'static,
{
    #[inline]
    fn reserve(&mut self, _additional: usize, _additional_values: usize) {}
}

// internal helper method to extend a mutable array
fn arrow_serialize_extend_internal<
    'a,
    A: 'static,
    T: ArrowSerialize + ArrowField<Type = A> + 'static,
    I: IntoIterator<Item = &'a A>,
>(
    into_iter: I,
    array: &mut <T as ArrowSerialize>::MutableArrayType,
) -> arrow2::error::Result<()> {
    let iter = into_iter.into_iter();
    array.reserve(iter.size_hint().0, 0);
    for i in iter {
        <T as ArrowSerialize>::arrow_serialize(i, array)?;
    }
    Ok(())
}

// internal helper method to serialize to a mutable array
fn arrow_serialize_internal<
    'a,
    A: 'static,
    T: ArrowSerialize + ArrowField<Type = A> + 'static,
    I: IntoIterator<Item = &'a A>,
>(
    into_iter: I,
) -> arrow2::error::Result<<T as ArrowSerialize>::MutableArrayType> {
    let mut arr = <T as ArrowSerialize>::new_array();
    arrow_serialize_extend_internal::<A, T, I>(into_iter, &mut arr)?;
    Ok(arr)
}

/// Top-level API to serialize to Arrow
pub trait TryIntoArrow<'a, ArrowArray, Element>
where
    Self: IntoIterator<Item = &'a Element>,
    Element: 'static,
{
    fn try_into_arrow(self) -> arrow2::error::Result<ArrowArray>;
    fn try_into_arrow_as_type<ArrowType>(self) -> arrow2::error::Result<ArrowArray>
    where
        ArrowType: ArrowSerialize + ArrowField<Type = Element> + 'static;
}

impl<'a, Element, Collection> TryIntoArrow<'a, Arc<dyn Array>, Element> for Collection
where
    Element: ArrowSerialize + ArrowField<Type = Element> + 'static,
    Collection: IntoIterator<Item = &'a Element>,
{
    fn try_into_arrow(self) -> arrow2::error::Result<Arc<dyn Array>> {
        Ok(arrow_serialize_internal::<Element, Element, Collection>(self)?.as_arc())
    }

    fn try_into_arrow_as_type<Field>(self) -> arrow2::error::Result<Arc<dyn Array>>
    where
        Field: ArrowSerialize + ArrowField<Type = Element> + 'static,
    {
        Ok(arrow_serialize_internal::<Element, Field, Collection>(self)?.as_arc())
    }
}

impl<'a, Element, Collection> TryIntoArrow<'a, Box<dyn Array>, Element> for Collection
where
    Element: ArrowSerialize + ArrowField<Type = Element> + 'static,
    Collection: IntoIterator<Item = &'a Element>,
{
    fn try_into_arrow(self) -> arrow2::error::Result<Box<dyn Array>> {
        Ok(arrow_serialize_internal::<Element, Element, Collection>(self)?.as_box())
    }

    fn try_into_arrow_as_type<E>(self) -> arrow2::error::Result<Box<dyn Array>>
    where
        E: ArrowSerialize + ArrowField<Type = Element> + 'static,
    {
        Ok(arrow_serialize_internal::<Element, E, Collection>(self)?.as_box())
    }
}

impl<'a, Element, Collection> TryIntoArrow<'a, Chunk<Arc<dyn Array>>, Element> for Collection
where
    Element: ArrowSerialize + ArrowField<Type = Element> + 'static,
    Collection: IntoIterator<Item = &'a Element>,
{
    fn try_into_arrow(self) -> arrow2::error::Result<Chunk<Arc<dyn Array>>> {
        Ok(Chunk::new(vec![arrow_serialize_internal::<
            Element,
            Element,
            Collection,
        >(self)?
        .as_arc()]))
    }

    fn try_into_arrow_as_type<Field>(self) -> arrow2::error::Result<Chunk<Arc<dyn Array>>>
    where
        Field: ArrowSerialize + ArrowField<Type = Element> + 'static,
    {
        Ok(Chunk::new(vec![arrow_serialize_internal::<
            Element,
            Field,
            Collection,
        >(self)?
        .as_arc()]))
    }
}

impl<'a, Element, Collection> TryIntoArrow<'a, Chunk<Box<dyn Array>>, Element> for Collection
where
    Element: ArrowSerialize + ArrowField<Type = Element> + 'static,
    Collection: IntoIterator<Item = &'a Element>,
{
    fn try_into_arrow(self) -> arrow2::error::Result<Chunk<Box<dyn Array>>> {
        Ok(Chunk::new(vec![arrow_serialize_internal::<
            Element,
            Element,
            Collection,
        >(self)?
        .as_box()]))
    }

    fn try_into_arrow_as_type<E>(self) -> arrow2::error::Result<Chunk<Box<dyn Array>>>
    where
        E: ArrowSerialize + ArrowField<Type = Element> + 'static,
    {
        Ok(Chunk::new(vec![arrow_serialize_internal::<
            Element,
            E,
            Collection,
        >(self)?
        .as_box()]))
    }
}
