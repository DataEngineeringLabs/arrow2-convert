//! Implementation and traits for mapping rust types to Arrow types

use arrow2::{
    buffer::Buffer,
    datatypes::{DataType, Field, TimeUnit},
    types::NativeType,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

/// Trait implemented by all types that can be used as an Arrow field.
///
/// Implementations are provided for types already supported by the arrow2 crate:
/// - numeric types: [`u8`], [`u16`], [`u32`], [`u64`], [`i8`], [`i16`], [`i32`], [`i128`], [`i64`], [`f32`], [`f64`],
/// - other types: [`bool`], [`String`]
/// - temporal types: [`chrono::NaiveDate`], [`chrono::NaiveTime`], [`chrono::NaiveDateTime`]
///
/// Custom implementations can be provided for other types.
///
/// The trait simply requires defining the [`ArrowField::data_type`]
///
/// Serialize and Deserialize functionality requires implementing the [`crate::ArrowSerialize`]
/// and the [`crate::ArrowDeserialize`] traits respectively.
pub trait ArrowField {
    /// This should be `Self` except when implementing large offset and fixed placeholder types.
    /// For the later, it should refer to the actual type. For example when the placeholder
    /// type is LargeString, this should be String.
    type Type;

    /// The [`DataType`]
    fn data_type() -> DataType;

    #[inline]
    #[doc(hidden)]
    /// For internal use and not meant to be reimplemented.
    /// returns the [`arrow2::datatypes::Field`] for this field
    fn field(name: &str) -> Field {
        Field::new(name, Self::data_type(), Self::is_nullable())
    }

    #[inline]
    #[doc(hidden)]
    /// For internal use and not meant to be reimplemented.
    /// Indicates that this field is nullable. This is reimplemented by the
    /// Option<T> blanket implementation.
    fn is_nullable() -> bool {
        false
    }
}

/// Enables the blanket implementations of [`Vec<T>`] as an Arrow field
/// if `T` is an Arrow field.
///
/// This tag is needed for [`Vec<u8>`] specialization, and can be obviated
/// once implementation specialization is available in rust.
#[macro_export]
macro_rules! arrow_enable_vec_for_type {
    ($t:ty) => {
        impl $crate::field::ArrowEnableVecForType for $t {}
    };
}
/// Marker used to allow [`Vec<T>`] to be used as a [`ArrowField`].
#[doc(hidden)]
pub trait ArrowEnableVecForType {}

// Macro to facilitate implementation for numeric types.
macro_rules! impl_numeric_type {
    ($physical_type:ty, $logical_type:ident) => {
        impl ArrowField for $physical_type {
            type Type = $physical_type;

            #[inline]
            fn data_type() -> arrow2::datatypes::DataType {
                arrow2::datatypes::DataType::$logical_type
            }
        }
    };
}

macro_rules! impl_numeric_type_full {
    ($physical_type:ty, $logical_type:ident) => {
        impl_numeric_type!($physical_type, $logical_type);
        arrow_enable_vec_for_type!($physical_type);
    };
}

// blanket implementation for optional fields
impl<T> ArrowField for Option<T>
where
    T: ArrowField,
{
    type Type = Option<<T as ArrowField>::Type>;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        <T as ArrowField>::data_type()
    }

    #[inline]
    fn is_nullable() -> bool {
        true
    }
}

// u8 does not get the full implementation since Vec<u8> and [u8] are considered binary.
impl_numeric_type!(u8, UInt8);
impl_numeric_type_full!(u16, UInt16);
impl_numeric_type_full!(u32, UInt32);
impl_numeric_type_full!(u64, UInt64);
impl_numeric_type_full!(i8, Int8);
impl_numeric_type_full!(i16, Int16);
impl_numeric_type_full!(i32, Int32);
impl_numeric_type_full!(i64, Int64);
impl_numeric_type_full!(arrow2::types::f16, Float16);
impl_numeric_type_full!(f32, Float32);
impl_numeric_type_full!(f64, Float64);

/// Maps a rust i128 to an Arrow Decimal where precision and scale are required.
pub struct I128<const PRECISION: usize, const SCALE: usize> {}

impl<const PRECISION: usize, const SCALE: usize> ArrowField for I128<PRECISION, SCALE> {
    type Type = i128;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Decimal(PRECISION, SCALE)
    }
}

impl ArrowField for String {
    type Type = String;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Utf8
    }
}

/// Represents the `LargeUtf8` Arrow type
pub struct LargeString {}

impl ArrowField for LargeString {
    type Type = String;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::LargeUtf8
    }
}

impl ArrowField for bool {
    type Type = Self;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Boolean
    }
}

impl ArrowField for NaiveDateTime {
    type Type = Self;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Timestamp(arrow2::datatypes::TimeUnit::Nanosecond, None)
    }
}

impl ArrowField for NaiveDate {
    type Type = Self;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Date32
    }
}

impl ArrowField for NaiveTime {
    type Type = Self;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Time32(TimeUnit::Second)
    }
}

impl ArrowField for Buffer<u8> {
    type Type = Self;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Binary
    }
}

impl ArrowField for Vec<u8> {
    type Type = Self;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Binary
    }
}

/// Represents the `LargeString` Arrow type.
pub struct LargeBinary {}

impl ArrowField for LargeBinary {
    type Type = Vec<u8>;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::LargeBinary
    }
}

/// Represents the `FixedSizeBinary` Arrow type.
pub struct FixedSizeBinary<const SIZE: usize> {}

impl<const SIZE: usize> ArrowField for FixedSizeBinary<SIZE> {
    type Type = Vec<u8>;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::FixedSizeBinary(SIZE)
    }
}

// Blanket implementation for Buffer
impl<T> ArrowField for Buffer<T>
where
    T: ArrowField + NativeType + ArrowEnableVecForType,
{
    type Type = Self;

    #[inline]
    fn data_type() -> DataType {
        DataType::List(Box::new(<T as ArrowField>::field("item")))
    }
}

// Blanket implementation for Vec.
impl<T> ArrowField for Vec<T>
where
    T: ArrowField + ArrowEnableVecForType,
{
    type Type = Vec<<T as ArrowField>::Type>;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::List(Box::new(<T as ArrowField>::field("item")))
    }
}

/// Represents the `LargeList` Arrow type.
pub struct LargeVec<T> {
    d: std::marker::PhantomData<T>,
}

impl<T> ArrowField for LargeVec<T>
where
    T: ArrowField + ArrowEnableVecForType,
{
    type Type = Vec<<T as ArrowField>::Type>;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::LargeList(Box::new(<T as ArrowField>::field("item")))
    }
}

/// Represents the `FixedSizeList` Arrow type.
pub struct FixedSizeVec<T, const SIZE: usize> {
    d: std::marker::PhantomData<T>,
}

impl<T, const SIZE: usize> ArrowField for FixedSizeVec<T, SIZE>
where
    T: ArrowField + ArrowEnableVecForType,
{
    type Type = Vec<<T as ArrowField>::Type>;

    #[inline]
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::FixedSizeList(Box::new(<T as ArrowField>::field("item")), SIZE)
    }
}

arrow_enable_vec_for_type!(String);
arrow_enable_vec_for_type!(LargeString);
arrow_enable_vec_for_type!(bool);
arrow_enable_vec_for_type!(NaiveDateTime);
arrow_enable_vec_for_type!(NaiveDate);
arrow_enable_vec_for_type!(NaiveTime);
arrow_enable_vec_for_type!(Vec<u8>);
arrow_enable_vec_for_type!(Buffer<u8>);
arrow_enable_vec_for_type!(LargeBinary);
impl<const SIZE: usize> ArrowEnableVecForType for FixedSizeBinary<SIZE> {}
impl<const PRECISION: usize, const SCALE: usize> ArrowEnableVecForType for I128<PRECISION, SCALE> {}

// Blanket implementation for Vec<Option<T>> if vectors are enabled for T
impl<T> ArrowEnableVecForType for Option<T> where T: ArrowField + ArrowEnableVecForType {}

// Blanket implementation for Vec<Vec<T>> and Vec<Buffer<T>> if vectors or buffers are enabled for T
impl<T> ArrowEnableVecForType for Vec<T> where T: ArrowField + ArrowEnableVecForType {}
impl<T> ArrowEnableVecForType for Buffer<T> where T: ArrowField + ArrowEnableVecForType {}
impl<T> ArrowEnableVecForType for LargeVec<T> where T: ArrowField + ArrowEnableVecForType {}
impl<T, const SIZE: usize> ArrowEnableVecForType for FixedSizeVec<T, SIZE> where
    T: ArrowField + ArrowEnableVecForType
{
}
