//! Implementation and traits for deserializing from Arrow.

use chrono::{NaiveDate, NaiveDateTime};

use crate::{field::*, physical_type::Nullable};

/// Implemented by [`ArrowField`] that can be deserialized from arrow
pub trait ArrowDeserialize: ArrowField + Sized {
    /// The `arrow2::Array` type corresponding to this field
    type Array: crate::physical_type::Array;

    /// Deserialize this field from arrow
    fn arrow_deserialize(
        v: <<Self::Array as crate::physical_type::Array>::Iter<'_> as Iterator>::Item,
    ) -> <Self as ArrowField>::Type;
}

// Macro to facilitate implementation for numeric types and numeric arrays.
macro_rules! impl_arrow_deserialize_primitive {
    ($physical_type:ty) => {
        impl ArrowDeserialize for $physical_type {
            type Array = $physical_type;

            #[inline]
            fn arrow_deserialize<'a>(v: &$physical_type) -> Self {
                *v
            }
        }
    };
}

// blanket implementation for optional fields
impl<T> ArrowDeserialize for Option<T>
where
    T: ArrowDeserialize,
    for<'a> Nullable<<T as ArrowDeserialize>::Array>: 'static
        + crate::physical_type::Array<
            Element<'a> = Option<
                <<T as ArrowDeserialize>::Array as crate::physical_type::Array>::Element<'a>,
            >,
        >,
{
    type Array = Nullable<<T as ArrowDeserialize>::Array>;

    #[inline]
    fn arrow_deserialize<'a>(
        v: Option<<<T as ArrowDeserialize>::Array as crate::physical_type::Array>::Element<'a>>,
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
    type Array = i128;

    #[inline]
    fn arrow_deserialize(v: &i128) -> i128 {
        *v
    }
}

impl ArrowDeserialize for String {
    type Array = crate::physical_type::Utf8<i32>;

    #[inline]
    fn arrow_deserialize(v: &str) -> String {
        v.into()
    }
}

impl<S> ArrowDeserialize for GenericUtf8<i32, S>
where
    for<'a> S: From<&'a str>,
{
    type Array = crate::physical_type::Utf8<i32>;

    #[inline]
    fn arrow_deserialize(v: &str) -> S {
        v.into()
    }
}

impl<S> ArrowDeserialize for GenericUtf8<i64, S>
where
    for<'a> S: From<&'a str>,
{
    type Array = crate::physical_type::Utf8<i64>;

    #[inline]
    fn arrow_deserialize(v: &str) -> S {
        v.into()
    }
}

impl ArrowDeserialize for bool {
    type Array = bool;

    #[inline]
    fn arrow_deserialize(v: bool) -> Self {
        v
    }
}

impl ArrowDeserialize for NaiveDateTime {
    type Array = i64;

    #[inline]
    fn arrow_deserialize(v: &i64) -> Self {
        arrow2::temporal_conversions::timestamp_ns_to_datetime(*v)
    }
}

impl ArrowDeserialize for NaiveDate {
    type Array = i32;

    #[inline]
    fn arrow_deserialize(v: &i32) -> Self {
        arrow2::temporal_conversions::date32_to_date(*v)
    }
}

impl<'a, C> ArrowDeserialize for GenericBinary<i32, C>
where
    Self: 'a,
    C: FromIterator<u8>,
    &'a C: IntoIterator<Item = &'a u8>,
{
    type Array = crate::physical_type::Binary<i32>;

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
    type Array = crate::physical_type::Binary<i64>;

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
    type Array = crate::physical_type::FixedSizeBinary<SIZE>;

    #[inline]
    fn arrow_deserialize(v: &[u8]) -> C {
        v.iter().map(|v| *v).collect()
    }
}

fn arrow_deserialize_collection<'a, T, C>(v: Box<dyn arrow2::array::Array>) -> Option<C>
where
    T: ArrowDeserialize + 'static,
    C: FromIterator<<T as ArrowField>::Type> + 'static,
{
    use std::ops::Deref;
    arrow_array_deserialize_iterator_internal::<<T as ArrowField>::Type, T>(v.deref())
        .map(|iter| iter.collect())
}

// Blanket implementation for Vec
impl<T> ArrowDeserialize for Vec<T>
where
    T: ArrowField<Type = T> + ArrowDeserialize + ArrowEnableVecForType + 'static,
{
    type Array = crate::physical_type::List<i32>;

    fn arrow_deserialize(v: Box<dyn arrow2::array::Array>) -> <Self as ArrowField>::Type {
        arrow_deserialize_collection::<T, Self>(v).unwrap_or_default()
    }
}

impl<'a, T, C> ArrowDeserialize for GenericList<i32, C, T>
where
    T: ArrowDeserialize + 'static,
    &'a C: IntoIterator<Item = &'a <T as ArrowField>::Type>,
    C: FromIterator<<T as ArrowField>::Type> + 'static + Default,
{
    type Array = crate::physical_type::List<i32>;

    fn arrow_deserialize(v: Box<dyn arrow2::array::Array>) -> <Self as ArrowField>::Type {
        arrow_deserialize_collection::<T, C>(v).unwrap_or_default()
    }
}

impl<'a, T, C> ArrowDeserialize for GenericList<i64, C, T>
where
    T: ArrowDeserialize + 'static,
    &'a C: IntoIterator<Item = &'a <T as ArrowField>::Type>,
    C: FromIterator<<T as ArrowField>::Type> + 'static + Default,
{
    type Array = crate::physical_type::List<i64>;

    fn arrow_deserialize(v: Box<dyn arrow2::array::Array>) -> <Self as ArrowField>::Type {
        arrow_deserialize_collection::<T, C>(v).unwrap_or_default()
    }
}

impl<'a, T, C, const SIZE: usize> ArrowDeserialize for FixedSizeList<C, T, SIZE>
where
    T: ArrowDeserialize + 'static,
    &'a C: IntoIterator<Item = &'a <T as ArrowField>::Type>,
    C: FromIterator<<T as ArrowField>::Type> + 'static + Default,
{
    type Array = crate::physical_type::FixedSizeList<SIZE>;

    fn arrow_deserialize(v: Box<dyn arrow2::array::Array>) -> <Self as ArrowField>::Type {
        arrow_deserialize_collection::<T, C>(v).unwrap_or_default()
    }
}

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
        ArrowType: ArrowDeserialize + ArrowField<Type = Element> + 'static;
}

/// Helper to return an iterator for elements from a [`arrow2::array::Array`].
fn arrow_array_deserialize_iterator_internal<Element, Field>(
    b: &dyn arrow2::array::Array,
) -> Option<impl Iterator<Item = Element> + '_>
where
    Field: ArrowDeserialize + ArrowField<Type = Element> + 'static,
{
    Some(
        <<Field as ArrowDeserialize>::Array as crate::physical_type::Array>::into_iter(b)?
            .map(<Field as ArrowDeserialize>::arrow_deserialize),
    )
}

/// Returns a typed iterator to a target type from an `arrow2::Array`
pub fn arrow_array_deserialize_iterator_as_type<Element, ArrowType>(
    arr: &dyn arrow2::array::Array,
) -> arrow2::error::Result<impl Iterator<Item = Element> + '_>
where
    Element: 'static,
    ArrowType: ArrowDeserialize + ArrowField<Type = Element> + 'static,
{
    if &<ArrowType as ArrowField>::data_type() != arr.data_type() {
        // TODO: use arrow2_convert error type here and include more detail
        Err(arrow2::error::Error::InvalidArgumentError(
            "Data type mismatch".to_string(),
        ))
    } else {
        arrow_array_deserialize_iterator_internal::<Element, ArrowType>(arr).ok_or_else(||
            // TODO: use arrow2_convert error type here and include more detail
            arrow2::error::Error::InvalidArgumentError("Schema mismatch".to_string()))
    }
}

/// Return an iterator that deserializes an [`Array`] to an element of type T
pub fn arrow_array_deserialize_iterator<T>(
    arr: &dyn arrow2::array::Array,
) -> arrow2::error::Result<impl Iterator<Item = T> + '_>
where
    T: ArrowDeserialize + ArrowField<Type = T> + 'static,
{
    arrow_array_deserialize_iterator_as_type::<T, T>(arr)
}

impl<Collection, Element, ArrowArray> TryIntoCollection<Collection, Element> for ArrowArray
where
    Element: ArrowDeserialize + ArrowField<Type = Element> + 'static,
    ArrowArray: std::borrow::Borrow<dyn arrow2::array::Array>,
    Collection: FromIterator<Element>,
{
    fn try_into_collection(self) -> arrow2::error::Result<Collection> {
        Ok(arrow_array_deserialize_iterator::<Element>(self.borrow())?.collect())
    }

    fn try_into_collection_as_type<ArrowType>(self) -> arrow2::error::Result<Collection>
    where
        ArrowType: ArrowDeserialize + ArrowField<Type = Element> + 'static,
    {
        Ok(
            arrow_array_deserialize_iterator_as_type::<Element, ArrowType>(self.borrow())?
                .collect(),
        )
    }
}
