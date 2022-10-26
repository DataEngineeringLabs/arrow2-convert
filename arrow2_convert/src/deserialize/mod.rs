//! Implementation and traits for deserializing from Arrow.

mod array_adapter_impls;
mod field_impls;

use crate::field::ArrowField;
use std::marker::PhantomData;

/// Implemented by array types to convert a `dyn arrow2::array::Array` to
/// either a value iterator or a default iterator over optional elements.
pub trait ArrayAdapter {
    /// The element of the array
    type Element<'a>;
    /// Iterator over the elements
    type Iter<'a>: Iterator<Item = Self::Element<'a>>;

    /// Convert to a typed iterator
    fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>>;
}

/// Implemented by fields that can be deserialized from arrow
pub trait ArrowDeserialize: ArrowField + Sized {
    /// The `arrow2::Array` type corresponding to this field
    type Array: ArrayAdapter;

    /// Deserialize this field from arrow
    fn arrow_deserialize(
        v: <Self::Array as ArrayAdapter>::Element<'_>,
    ) -> <Self as ArrowField>::Type;
}

/// Wrapper to implement ArrayAdapter for an iterator over optional elements
pub struct Nullable<T> {
    _t: PhantomData<T>,
}

/// Top-level API to deserialize from Arrow, represented by arrow2 data-structures
/// This is implemented by wrappers around arrow2 arrays such as Box<dyn Array>
pub trait TryIntoCollection<Collection, Element>
where
    Element: ArrowField,
    Collection: FromIterator<Element>,
{
    /// Convert from a `arrow2::Array` to any collection that implements the `FromIterator` trait
    fn try_into_collection(self) -> arrow2::error::Result<Collection>;

    /// Same as `try_into_collection` but can coerce the conversion to a specific Arrow type. This is
    /// useful for using fixed-size and large offset arrow types.
    fn try_into_collection_as_type<ArrowType>(self) -> arrow2::error::Result<Collection>
    where
        ArrowType: ArrowDeserialize + ArrowField<Type = Element> + 'static;
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
        array_to_typed_iter::<ArrowType>(arr).ok_or_else(||
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

fn array_to_typed_iter<'a, T>(
    array: &'a dyn arrow2::array::Array,
) -> Option<impl Iterator<Item = <T as ArrowField>::Type> + 'a>
where
    T: ArrowDeserialize + 'a,
{
    let iter = <<T as ArrowDeserialize>::Array as ArrayAdapter>::into_iter(array)?;
    Some(iter.map(T::arrow_deserialize))
}

fn array_to_collection<'a, T, C>(array: &'a dyn arrow2::array::Array) -> C
where
    T: ArrowDeserialize + 'a,
    C: FromIterator<<T as ArrowField>::Type> + Default,
{
    array_to_typed_iter::<T>(array)
        .map(|iter| iter.collect())
        .unwrap_or_default()
}
