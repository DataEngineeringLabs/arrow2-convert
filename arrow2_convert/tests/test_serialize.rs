use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2_convert::field::{ArrowField, FixedSizeBinary};
use arrow2_convert::serialize::*;
use std::sync::Arc;

#[test]
fn test_error_exceed_fixed_size_binary() {
    let strs = [b"abc".to_vec()];
    let r: arrow2::error::Result<Box<dyn Array>> =
        strs.try_into_arrow_as_type::<FixedSizeBinary<2>>();
    assert!(r.is_err())
}

#[test]
fn test_chunk() {
    let strs = [b"abc".to_vec()];
    let r: Chunk<Box<dyn Array>> = strs.try_into_arrow_as_type::<FixedSizeBinary<3>>().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(
        r.arrays()[0].data_type(),
        &<FixedSizeBinary<3> as ArrowField>::data_type()
    );

    let r: Chunk<Box<dyn Array>> = strs.try_into_arrow().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(
        r.arrays()[0].data_type(),
        &<Vec<u8> as ArrowField>::data_type()
    );

    let r: Chunk<Arc<dyn Array>> = strs.try_into_arrow_as_type::<FixedSizeBinary<3>>().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(
        r.arrays()[0].data_type(),
        &<FixedSizeBinary<3> as ArrowField>::data_type()
    );

    let r: Chunk<Arc<dyn Array>> = strs.try_into_arrow().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(
        r.arrays()[0].data_type(),
        &<Vec<u8> as ArrowField>::data_type()
    );
}

#[test]
fn test_array() {
    let strs = [b"abc".to_vec()];
    let r: Box<dyn Array> = strs.try_into_arrow_as_type::<FixedSizeBinary<3>>().unwrap();
    assert_eq!(
        r.data_type(),
        &<FixedSizeBinary<3> as ArrowField>::data_type()
    );

    let r: Box<dyn Array> = strs.try_into_arrow().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r.data_type(), &<Vec<u8> as ArrowField>::data_type());

    let r: Arc<dyn Array> = strs.try_into_arrow_as_type::<FixedSizeBinary<3>>().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(
        r.data_type(),
        &<FixedSizeBinary<3> as ArrowField>::data_type()
    );

    let r: Arc<dyn Array> = strs.try_into_arrow().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r.data_type(), &<Vec<u8> as ArrowField>::data_type());
}

#[test]
fn test_field_serialize_error() {
    pub struct CustomType(u64);

    impl arrow2_convert::field::ArrowField for CustomType {
        type Type = Self;

        #[inline]
        fn data_type() -> arrow2::datatypes::DataType {
            arrow2::datatypes::DataType::Extension(
                "custom".to_string(),
                Box::new(arrow2::datatypes::DataType::UInt64),
                None,
            )
        }
    }

    impl arrow2_convert::serialize::ArrowSerialize for CustomType {
        type MutableArrayType = arrow2::array::MutablePrimitiveArray<u64>;

        #[inline]
        fn new_array() -> Self::MutableArrayType {
            Self::MutableArrayType::from(<Self as arrow2_convert::field::ArrowField>::data_type())
        }

        #[inline]
        fn arrow_serialize(_: &Self, _: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
            Err(arrow2::error::Error::NotYetImplemented("".to_owned()))
        }
    }

    impl arrow2_convert::deserialize::ArrowDeserialize for CustomType {
        type ArrayType = arrow2::array::PrimitiveArray<u64>;

        #[inline]
        fn arrow_deserialize(v: Option<&u64>) -> Option<Self> {
            v.map(|t| CustomType(*t))
        }
    }

    let arr = vec![CustomType(0)];
    let r: arrow2::error::Result<Box<dyn Array>> = arr.try_into_arrow();
    assert!(r.is_err())
}
