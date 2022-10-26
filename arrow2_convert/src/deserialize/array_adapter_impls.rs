use arrow2::{
    array::*,
    bitmap::utils::BitmapIter,
    types::{NativeType, Offset},
};

use super::{ArrayAdapter, Nullable};

impl<T: NativeType> ArrayAdapter for PrimitiveArray<T> {
    type Element<'a> = &'a T;
    type Iter<'a> = std::slice::Iter<'a, T>;

    #[inline]
    fn into_iter(array: &dyn Array) -> Option<Self::Iter<'_>> {
        Some(
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()?
                .values_iter(),
        )
    }
}

impl<T> ArrayAdapter for Nullable<T>
where
    T: ArrayAdapter + arrow2::array::Array,
    for<'a> &'a T: IntoIterator<Item = Option<<T as ArrayAdapter>::Element<'a>>>,
{
    type Element<'a> = Option<<T as ArrayAdapter>::Element<'a>>;
    type Iter<'a> = <&'a T as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
        let array = array.as_any().downcast_ref::<T>()?;
        Some(<&T as IntoIterator>::into_iter(array))
    }
}

macro_rules! impl_into_value_iter {
    () => {
        #[inline]
        fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
            Some(array.as_any().downcast_ref::<Self>()?.values_iter())
        }
    };
}

impl<O: Offset> ArrayAdapter for Utf8Array<O> {
    type Element<'a> = &'a str;
    type Iter<'a> = Utf8ValuesIter<'a, O>;
    impl_into_value_iter!();
}

impl ArrayAdapter for BooleanArray {
    type Element<'a> = bool;
    type Iter<'a> = BitmapIter<'a>;
    impl_into_value_iter!();
}

impl<O: Offset> ArrayAdapter for BinaryArray<O> {
    type Element<'a> = &'a [u8];
    type Iter<'a> = BinaryValueIter<'a, O>;
    impl_into_value_iter!();
}

impl ArrayAdapter for FixedSizeBinaryArray {
    type Element<'a> = &'a [u8];
    type Iter<'a> = std::slice::ChunksExact<'a, u8>;
    impl_into_value_iter!();
}

impl<O: Offset> ArrayAdapter for ListArray<O> {
    type Element<'a> = Box<dyn Array>;
    type Iter<'a> = ArrayValuesIter<'a, ListArray<O>>;
    impl_into_value_iter!();
}

impl ArrayAdapter for FixedSizeListArray {
    type Element<'a> = Box<dyn Array>;
    type Iter<'a> = ArrayValuesIter<'a, FixedSizeListArray>;
    impl_into_value_iter!();
}
