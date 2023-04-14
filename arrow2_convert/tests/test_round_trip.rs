use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2_convert::deserialize::arrow_array_deserialize_iterator_as_type;
use arrow2_convert::deserialize::*;
use arrow2_convert::field::{LargeBinary, I128};
use arrow2_convert::serialize::*;
use arrow2_convert::{
    field::{FixedSizeBinary, FixedSizeVec, LargeString, LargeVec},
    ArrowDeserialize, ArrowField, ArrowSerialize,
};
use std::borrow::Borrow;
use std::f32::INFINITY;
use std::sync::Arc;

#[test]
fn test_nested_optional_struct_array() {
    #[derive(Debug, Clone, ArrowField, ArrowSerialize, ArrowDeserialize, PartialEq)]
    struct Top {
        child_array: Vec<Option<Child>>,
    }
    #[derive(Debug, Clone, ArrowField, ArrowSerialize, ArrowDeserialize, PartialEq)]
    struct Child {
        a1: i64,
    }

    let original_array = vec![
        Top {
            child_array: vec![
                Some(Child { a1: 10 }),
                None,
                Some(Child { a1: 12 }),
                Some(Child { a1: 14 }),
            ],
        },
        Top {
            child_array: vec![None, None, None, None],
        },
        Top {
            child_array: vec![None, None, Some(Child { a1: 12 }), None],
        },
    ];

    let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<Top> = b.try_into_collection().unwrap();
    assert_eq!(original_array, round_trip);
}

#[test]
fn test_large_string() {
    let strs = vec!["1".to_string(), "2".to_string()];
    let b: Box<dyn Array> = strs.try_into_arrow_as_type::<LargeString>().unwrap();
    assert_eq!(b.data_type(), &DataType::LargeUtf8);
    let round_trip: Vec<String> = b.try_into_collection_as_type::<LargeString>().unwrap();
    assert_eq!(round_trip, strs);
}

#[test]
fn test_large_string_nested() {
    let strs = [vec!["1".to_string(), "2".to_string()]];
    let b: Box<dyn Array> = strs.try_into_arrow_as_type::<Vec<LargeString>>().unwrap();
    assert_eq!(
        b.data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, false)))
    );
    let round_trip: Vec<Vec<String>> = b.try_into_collection_as_type::<Vec<LargeString>>().unwrap();
    assert_eq!(round_trip, strs);
}

#[test]
fn test_large_binary() {
    let strs = [b"abc".to_vec()];
    let b: Box<dyn Array> = strs.try_into_arrow_as_type::<LargeBinary>().unwrap();
    assert_eq!(b.data_type(), &DataType::LargeBinary);
    let round_trip: Vec<Vec<u8>> = b.try_into_collection_as_type::<LargeBinary>().unwrap();
    assert_eq!(round_trip, strs);
}

#[test]
fn test_large_binary_nested() {
    let strs = [vec![b"abc".to_vec(), b"abd".to_vec()]];
    let b: Box<dyn Array> = strs.try_into_arrow_as_type::<Vec<LargeBinary>>().unwrap();
    assert_eq!(
        b.data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, false)))
    );
    let round_trip: Vec<Vec<Vec<u8>>> =
        b.try_into_collection_as_type::<Vec<LargeBinary>>().unwrap();
    assert_eq!(round_trip, strs);
}

#[test]
fn test_fixed_size_binary() {
    let strs = [b"abc".to_vec()];
    let b: Box<dyn Array> = strs.try_into_arrow_as_type::<FixedSizeBinary<3>>().unwrap();
    assert_eq!(b.data_type(), &DataType::FixedSizeBinary(3));
    let round_trip: Vec<Vec<u8>> = b
        .try_into_collection_as_type::<FixedSizeBinary<3>>()
        .unwrap();
    assert_eq!(round_trip, strs);
}

#[test]
fn test_large_vec() {
    let ints = vec![vec![1, 2, 3]];
    let b: Box<dyn Array> = ints.try_into_arrow_as_type::<LargeVec<i32>>().unwrap();
    assert_eq!(
        b.data_type(),
        &DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, false)))
    );
    let round_trip: Vec<Vec<i32>> = b.try_into_collection_as_type::<LargeVec<i32>>().unwrap();
    assert_eq!(round_trip, ints);
}

#[test]
fn test_large_vec_nested() {
    let strs = [vec![b"abc".to_vec(), b"abd".to_vec()]];
    let b: Box<dyn Array> = strs
        .try_into_arrow_as_type::<LargeVec<LargeBinary>>()
        .unwrap();
    assert_eq!(
        b.data_type(),
        &DataType::LargeList(Arc::new(Field::new("item", DataType::LargeBinary, false)))
    );
    let round_trip: Vec<Vec<Vec<u8>>> = b
        .try_into_collection_as_type::<LargeVec<LargeBinary>>()
        .unwrap();
    assert_eq!(round_trip, strs);
}

#[test]
fn test_fixed_size_vec() {
    let ints = vec![vec![1, 2, 3]];
    let b: Box<dyn Array> = ints
        .try_into_arrow_as_type::<FixedSizeVec<i32, 3>>()
        .unwrap();
    assert_eq!(
        b.data_type(),
        &DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 3)
    );
    let round_trip: Vec<Vec<i32>> = b
        .try_into_collection_as_type::<FixedSizeVec<i32, 3>>()
        .unwrap();
    assert_eq!(round_trip, ints);
}

#[test]
fn test_primitive_type_vec() {
    macro_rules! test_int_type {
        ($t:ty) => {
            let original_array = vec![1 as $t, 2, 3];
            let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
            let round_trip: Vec<$t> = b.try_into_collection().unwrap();
            assert_eq!(original_array, round_trip);

            let original_array = vec![Some(1 as $t), None, Some(3)];
            let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
            let round_trip: Vec<Option<$t>> = b.try_into_collection().unwrap();
            assert_eq!(original_array, round_trip);

            let original_array = vec![Some(1 as $t), None, Some(3)];
            let b: Arc<dyn Array> = original_array.try_into_arrow().unwrap();
            let round_trip: Vec<Option<$t>> =
                b.try_into_collection_as_type::<Option<$t>>().unwrap();
            assert_eq!(original_array, round_trip);
        };
    }

    macro_rules! test_float_type {
        ($t:ty) => {
            let original_array = vec![1 as $t, 2., 3.];
            let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
            let round_trip: Vec<$t> = b.try_into_collection().unwrap();
            assert_eq!(original_array, round_trip);

            let original_array = vec![Some(1 as $t), None, Some(3.)];
            let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
            let round_trip: Vec<Option<$t>> = b.try_into_collection().unwrap();
            assert_eq!(original_array, round_trip);

            let original_array = vec![Some(1 as $t), None, Some(3.)];
            let b: Arc<dyn Array> = original_array.try_into_arrow().unwrap();
            let round_trip: Vec<Option<$t>> = b.try_into_collection().unwrap();
            assert_eq!(original_array, round_trip);
        };
    }

    test_int_type!(i8);
    test_int_type!(i16);
    test_int_type!(i32);
    test_int_type!(i64);
    test_int_type!(u8);
    test_int_type!(u16);
    test_int_type!(u32);
    test_int_type!(u64);
    test_float_type!(f32);
    test_float_type!(f64);

    // `arrow2::types::f16` isn't a native type so we can't just use `as`
    {
        let original_array: Vec<arrow2::types::f16> =
            vec![1.0, 2.5, 47800.0, 0.000012, -0.0, 0.0, INFINITY]
                .iter()
                .map(|f| arrow2::types::f16::from_f32(*f))
                .collect();
        let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
        let round_trip: Vec<arrow2::types::f16> = b.try_into_collection().unwrap();
        assert_eq!(original_array, round_trip);

        let original_array: Vec<Option<arrow2::types::f16>> = vec![Some(1.), None, Some(3.)]
            .iter()
            .map(|f| f.map(arrow2::types::f16::from_f32))
            .collect();
        let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
        let round_trip: Vec<Option<arrow2::types::f16>> = b.try_into_collection().unwrap();
        assert_eq!(original_array, round_trip);

        let original_array: Vec<Option<arrow2::types::f16>> = vec![Some(1.), None, Some(3.)]
            .iter()
            .map(|f| f.map(arrow2::types::f16::from_f32))
            .collect();
        let b: Arc<dyn Array> = original_array.try_into_arrow().unwrap();
        let round_trip: Vec<Option<arrow2::types::f16>> = b.try_into_collection().unwrap();
        assert_eq!(original_array, round_trip);
    };

    // i128
    // i128 is special since we need to require precision and scale so the TryIntoArrow trait
    // is not implemented for Vec<i128>.
    let original_array = vec![1_i128, 2, 3];
    let b: Box<dyn Array> = arrow_serialize_to_mutable_array::<_, I128<32, 32>, _>(&original_array)
        .unwrap()
        .as_box();
    let round_trip: Vec<i128> =
        arrow_array_deserialize_iterator_as_type::<_, I128<32, 32>>(b.borrow())
            .unwrap()
            .collect();
    assert_eq!(original_array, round_trip);

    let original_array = vec![Some(1_i128), None, Some(3)];
    let b: Box<dyn Array> =
        arrow_serialize_to_mutable_array::<_, Option<I128<32, 32>>, _>(&original_array)
            .unwrap()
            .as_box();
    let round_trip: Vec<Option<i128>> =
        arrow_array_deserialize_iterator_as_type::<_, Option<I128<32, 32>>>(b.borrow())
            .unwrap()
            .collect();
    assert_eq!(original_array, round_trip);

    // bool
    let original_array = vec![false, true, false];
    let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<bool> = b.try_into_collection().unwrap();
    assert_eq!(original_array, round_trip);

    let original_array = vec![Some(false), Some(true), None];
    let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<Option<bool>> = b.try_into_collection().unwrap();
    assert_eq!(original_array, round_trip);

    let original_array = vec![Some(b"aa".to_vec()), None];
    let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
    let round_trip: Vec<Option<Vec<u8>>> = b.try_into_collection().unwrap();
    assert_eq!(original_array, round_trip);
}

#[test]
fn test_escaped_name() {
    #[derive(ArrowField, ArrowSerialize, ArrowDeserialize, Debug, Eq, PartialEq)]
    struct EscapedName {
        r#type: bool,
    }
    let array = [EscapedName { r#type: true }, EscapedName { r#type: false }];
    let b: Box<dyn Array> = array.try_into_arrow().unwrap();
    let ty = b.data_type();
    match ty {
        DataType::Struct(s) => {
            assert_eq!(s[0].name, "type");
        }
        _ => unreachable!(),
    }
    let round_trip: Vec<EscapedName> = b.try_into_collection().unwrap();
    assert_eq!(array.as_slice(), round_trip.as_slice());
}
