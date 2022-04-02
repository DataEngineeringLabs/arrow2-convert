use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2_convert::deserialize::*;
use arrow2_convert::field::LargeBinary;
use arrow2_convert::serialize::*;
use arrow2_convert::{
    field::{FixedSizeBinary, FixedSizeVec, LargeString, LargeVec},
    ArrowField,
};

#[test]
fn test_nested_optional_struct_array() {
    #[derive(Debug, Clone, ArrowField, PartialEq)]
    struct Top {
        child_array: Vec<Option<Child>>,
    }
    #[derive(Debug, Clone, ArrowField, PartialEq)]
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
        &DataType::List(Box::new(Field::new("item", DataType::LargeUtf8, false)))
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
        &DataType::List(Box::new(Field::new("item", DataType::LargeBinary, false)))
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
        &DataType::LargeList(Box::new(Field::new("item", DataType::Int32, false)))
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
        &DataType::LargeList(Box::new(Field::new("item", DataType::LargeBinary, false)))
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
        &DataType::FixedSizeList(Box::new(Field::new("item", DataType::Int32, false)), 3)
    );
    let round_trip: Vec<Vec<i32>> = b
        .try_into_collection_as_type::<FixedSizeVec<i32, 3>>()
        .unwrap();
    assert_eq!(round_trip, ints);
}
