//! Implementations and traits for types have unique physical representations in the arrow format.

use std::marker::PhantomData;

use arrow2::{
    array::{
        ArrayValuesIter, BinaryArray, BinaryValueIter, BooleanArray, FixedSizeBinaryArray,
        FixedSizeListArray, ListArray, MutableBinaryArray, MutableBooleanArray, MutableListArray,
        MutablePrimitiveArray, MutableUtf8Array, Utf8Array, Utf8ValuesIter,
    },
    bitmap::utils::BitmapIter,
};


/// Implemented by physical types convert to the corresponding mutable array types. This is
/// used to differentiate between arrow2 value iterators (which are used for required
/// fields), and the default iterators, which are used by optional fields.
pub trait MutableArrayAdapter {
    /// The element of the array
    type Element<'a>;
    /// The MutableArray implementation
    type Array;

    /// Create a new mutable array
    fn new_array() -> Self::Array;
    /// Push an element into the mutable array.
    fn try_push(array: &mut Self::Array, element: Self::Element<'_>) -> arrow2::error::Result<()>;
}

/// A physical type that's nullable
#[derive(Default)]
pub struct NullableArray<T>
where
    T: Array,
{
    _data: PhantomData<T>,
}

macro_rules! declare_offset_type {
    ($name: ident) => {
        #[derive(Default)]
        /// Physical type for $name
        pub struct $name<O: arrow2::types::Offset> {
            _data: PhantomData<O>,
        }
    };
}

declare_offset_type!(Utf8);

declare_offset_type!(Binary);
/// Represents the `FixedSizeBinary` Arrow type.
#[derive(Default)]
pub struct FixedSizeBinary<const SIZE: usize> {}

declare_offset_type!(List);
/// Represents the `FixedSizeList` arrow type
#[derive(Default)]
pub struct FixedSizeList<const SIZE: usize> {}

macro_rules! impl_physical_type_generic {
    ($t:ty, $element_type:ty, $array_type:ty, $iter_type:ty, $mutable_array_type:ty) => {
        impl ArrayAdapter for $t {
            type Element<'a> = $element_type;
            type Iter<'a> = $iter_type;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.values_iter())
            }
        }

        impl ArrayAdapter for NullableArray<$t> {
            type Element<'a> = Option<$element_type>;
            type Iter<'a> = <&'a $array_type as IntoIterator>::IntoIter;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.iter())
            }
        }

        impl MutableArrayAdapter for $t {
            type Element<'a> = $t;
            type Array = $mutable_array_type;

            fn new_array() -> Self::Array {
                Self::Array::new()
            }

            fn try_push(array: &mut Self::Array, e: $t) -> arrow2::error::Result<()> {
                use arrow2::array::TryPush;
                array.try_push(Some(e))
            }
        }
    };
}

// TODO: consolidate with above macro with generic type bounds. didn't figure out a way
// to include where bounds in macros by example.
macro_rules! impl_physical_type_with_offset {
    ($t:ty, $element_type:ty, $array_type:ty, $iter_type:ty, $mutable_array_type:ty) => {
        impl<O: arrow2::types::Offset> $crate::physical_type::PhysicalType for $t {}

        impl<O: arrow2::types::Offset> ArrayAdapter for $t {
            type Element<'a> = $element_type;
            type Iter<'a> = $iter_type;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.values_iter())
            }
        }

        impl<O: arrow2::types::Offset> ArrayAdapter for Nullable<$t> {
            type Element<'a> = Option<$element_type>;
            type Iter<'a> = <&'a $array_type as IntoIterator>::IntoIter;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.iter())
            }
        }

        impl<O: arrow2::types::Offset> MutableArrayAdapter for $t {
            type Element<'a> = $element_type;
            type Array = $mutable_array_type;

            fn new_array() -> Self::Array {
                Self::Array::new()
            }

            fn try_push<'a>(
                array: &mut Self::Array,
                e: $element_type,
            ) -> arrow2::error::Result<()> {
                use arrow2::array::TryPush;
                array.try_push(Some(e))
            }
        }
    };
}

// TODO: consolidate with above macro with generic type bounds. didn't figure out a way
// to include where bounds in macros by example.
macro_rules! impl_physical_type_with_size {
    ($t:ty, $element_type:ty, $array_type:ty, $iter_type:ty) => {
        impl<const SIZE: usize> $crate::physical_type::PhysicalType for $t {}

        impl<const SIZE: usize> ArrayAdapter for $t {
            type Element<'a> = $element_type;
            type Iter<'a> = $iter_type;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.values_iter())
            }
        }

        impl<const SIZE: usize> ArrayAdapter for Nullable<$t> {
            type Element<'a> = Option<$element_type>;
            type Iter<'a> = <&'a $array_type as IntoIterator>::IntoIter;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.iter())
            }
        }
    };
}

macro_rules! impl_numeric_type {
    ($t:ty) => {
        impl_physical_type_generic!(
            $t,
            &'a $t,
            arrow2::array::PrimitiveArray<$t>,
            std::slice::Iter<'a, $t>,
            MutablePrimitiveArray<$t>
        );
    };
}

impl<T: PhysicalType> PhysicalType for NullableArray<T> {}

impl_numeric_type!(u8);
impl_numeric_type!(u16);
impl_numeric_type!(u32);
impl_numeric_type!(u64);
impl_numeric_type!(i8);
impl_numeric_type!(i16);
impl_numeric_type!(i32);
impl_numeric_type!(i64);
impl_numeric_type!(i128);
impl_numeric_type!(f32);
impl_numeric_type!(f64);

impl_physical_type_generic!(
    bool,
    bool,
    BooleanArray,
    BitmapIter<'a>,
    MutableBooleanArray
);

impl_physical_type_with_offset!(
    Utf8<O>,
    &'a str,
    Utf8Array<O>,
    Utf8ValuesIter<'a, O>,
    MutableUtf8Array<O>
);

impl_physical_type_with_offset!(
    Binary<O>,
    &'a [u8],
    BinaryArray<O>,
    BinaryValueIter<'a, O>,
    MutableBinaryArray<O>
);

impl_physical_type_with_size!(
    FixedSizeBinary<SIZE>,
    &'a [u8],
    FixedSizeBinaryArray,
    std::slice::ChunksExact<'a, u8>
);

impl_physical_type_with_offset!(
    List<O>,
    Box<dyn arrow2::array::Array>,
    ListArray<O>,
    ArrayValuesIter<'a, ListArray<O>>,
    MutableListArray<O, >
);

impl_physical_type_with_size!(
    FixedSizeList<SIZE>,
    Box<dyn arrow2::array::Array>,
    FixedSizeListArray,
    ArrayValuesIter<'a, FixedSizeListArray>
);
