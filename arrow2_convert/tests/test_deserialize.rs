use arrow2::error::Result;
use arrow2::{array::*, buffer::Buffer};
use arrow2_convert::{deserialize::*, serialize::*, ArrowDeserialize, ArrowField, ArrowSerialize};

#[test]
fn test_deserialize_iterator() {
    use arrow2::array::*;
    use arrow2_convert::deserialize::*;
    use arrow2_convert::serialize::*;
    use std::borrow::Borrow;

    #[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct S {
        a1: i64,
    }

    let original_array = [S { a1: 1 }, S { a1: 100 }, S { a1: 1000 }];
    let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
    let iter = arrow_array_deserialize_iterator::<S>(b.borrow()).unwrap();
    for (i, k) in iter.zip(original_array.iter()) {
        assert_eq!(&i, k);
    }

    let original_array = [Some(Some(1_i32)), Some(Some(100)), Some(None), None];
    let expected = [Some(Some(1_i32)), Some(Some(100)), None, None];
    let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
    let iter = arrow_array_deserialize_iterator::<Option<Option<i32>>>(b.borrow()).unwrap();
    for (i, k) in iter.zip(expected.iter()) {
        assert_eq!(&i, k);
    }
}

#[test]
fn test_deserialize_schema_mismatch_error() {
    #[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct S1 {
        a: i64,
    }
    #[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct S2 {
        a: String,
    }

    let arr1 = vec![S1 { a: 1 }, S1 { a: 2 }];
    let arr1: Box<dyn Array> = arr1.try_into_arrow().unwrap();
    let result: Result<Vec<S2>> = arr1.try_into_collection();
    assert!(result.is_err());

    let arr1 = vec![S1 { a: 1 }, S1 { a: 2 }];
    let arr1: Box<dyn Array> = arr1.try_into_arrow().unwrap();
    let result: Result<Vec<_>> = arr1.try_into_collection_as_type::<S2>();
    assert!(result.is_err());
}

#[test]
fn test_deserialize_large_types_schema_mismatch_error() {
    #[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct S1 {
        a: String,
    }
    #[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
    struct S2 {
        #[arrow_field(type = "arrow2_convert::field::LargeString")]
        a: String,
    }

    let arr1 = vec![
        S1 {
            a: "123".to_string(),
        },
        S1 {
            a: "333".to_string(),
        },
    ];
    let arr1: Box<dyn Array> = arr1.try_into_arrow().unwrap();

    let result: Result<Vec<S2>> = arr1.try_into_collection();
    assert!(result.is_err());
}

#[test]
fn test_deserialize_buffer() {
    let original_array = [Buffer::from_iter(0u16..5), Buffer::from_iter(7..9)];
    let b: Box<dyn Array> = original_array.try_into_arrow().unwrap();
    let iter = arrow_array_deserialize_iterator::<Buffer<u16>>(b.as_ref()).unwrap();
    for (i, k) in iter.zip(original_array.iter()) {
        assert_eq!(&i, k);
    }
}
