//! Implementations and traits for types have unique physical representations in the arrow format.

use std::marker::PhantomData;

use arrow2::{
    array::{
        ArrayValuesIter, BinaryArray, BinaryValueIter, BooleanArray, FixedSizeBinaryArray,
        FixedSizeListArray, ListArray, Utf8Array, Utf8ValuesIter,
    },
    bitmap::utils::BitmapIter,
};

/// Implemented by types that have corresponding `arrow2::Array` and `arrow2::MutableArray`
/// implementations, that natively store elements of this type.
///
/// Container (struct, enum) types that are annotated by the ArrowSerialize and ArrowDeserialize derive
/// macros, and have auto-generated `Array` and `MutableArray` definitions
/// are also considered physical types.
pub trait PhysicalType {}

/// Implemented by physical types convert to the corresponding array types. This is
/// used to differentiate between arrow2 value iterators (which are used for required
/// fields), and the default iterators, which are used by optional fields.
pub trait Array: PhysicalType {
    /// The element of the array
    type Element<'a>
    where
        Self: 'a;
    /// Iterator over the elements
    type Iter<'a>: Iterator<Item = Self::Element<'a>>
    where
        Self: 'a;

    /// Convert an untyped array into an iterator
    fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>>;
}

/// Implemented by physical types convert to the corresponding mutable array types. This is
/// used to differentiate between arrow2 value iterators (which are used for required
/// fields), and the default iterators, which are used by optional fields.
pub trait MutableArray: PhysicalType {
    /// The element of the array
    type Element;
    /// The MutableArray implemenation
    type Array: arrow2::array::TryPush<Option<Self::Element>>;

    /// Create a new mutable array
    fn new_array() -> Self::Array;
    /// Push an element into the mutable array.
    fn try_push(array: &mut Self::Array, repr: Self::Element);
}

/// A physical type that's nullable
#[derive(Default)]
pub struct Nullable<T>
where
    T: PhysicalType,
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
    ($t:ty, $element_type:ty, $array_type:ty, $iter_type:ty) => {
        impl $crate::physical_type::PhysicalType for $t {}

        impl Array for $t {
            type Element<'a> = $element_type;
            type Iter<'a> = $iter_type;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.values_iter())
            }
        }

        impl Array for Nullable<$t> {
            type Element<'a> = Option<$element_type>;
            type Iter<'a> = <&'a $array_type as IntoIterator>::IntoIter;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.iter())
            }
        }
    };
}

// TODO: consolidate with above macro with generic type bounds. didn't figure out a way
// to include where bounds in macros by example.
macro_rules! impl_physical_type_with_offset {
    ($t:ty, $element_type:ty, $array_type:ty, $iter_type:ty) => {
        impl<O: arrow2::types::Offset> $crate::physical_type::PhysicalType for $t {}

        impl<O: arrow2::types::Offset> Array for $t {
            type Element<'a> = $element_type;
            type Iter<'a> = $iter_type;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.values_iter())
            }
        }

        impl<O: arrow2::types::Offset> Array for Nullable<$t> {
            type Element<'a> = Option<$element_type>;
            type Iter<'a> = <&'a $array_type as IntoIterator>::IntoIter;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.iter())
            }
        }
    };
}

// TODO: consolidate with above macro with generic type bounds. didn't figure out a way
// to include where bounds in macros by example.
macro_rules! impl_physical_type_with_size {
    ($t:ty, $element_type:ty, $array_type:ty, $iter_type:ty) => {
        impl<const SIZE: usize> $crate::physical_type::PhysicalType for $t {}

        impl<const SIZE: usize> Array for $t {
            type Element<'a> = $element_type;
            type Iter<'a> = $iter_type;

            fn into_iter(array: &dyn arrow2::array::Array) -> Option<Self::Iter<'_>> {
                Some(array.as_any().downcast_ref::<$array_type>()?.values_iter())
            }
        }

        impl<const SIZE: usize> Array for Nullable<$t> {
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
            std::slice::Iter<'a, $t>
        );
    };
}

impl<T: PhysicalType> PhysicalType for Nullable<T> {}

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

impl_physical_type_generic!(bool, bool, BooleanArray, BitmapIter<'a>);
impl_physical_type_with_offset!(Utf8<O>, &'a str, Utf8Array<O>, Utf8ValuesIter<'a, O>);
impl_physical_type_with_offset!(Binary<O>, &'a [u8], BinaryArray<O>, BinaryValueIter<'a, O>);

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
    ArrayValuesIter<'a, ListArray<O>>
);

impl_physical_type_with_size!(
    FixedSizeList<SIZE>,
    Box<dyn arrow2::array::Array>,
    FixedSizeListArray,
    ArrayValuesIter<'a, FixedSizeListArray>
);
