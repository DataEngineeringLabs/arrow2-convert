use arrow2::array::*;
use arrow2_convert::{deserialize::TryIntoCollection, serialize::TryIntoArrow, ArrowField};

#[test]
fn test_dense_enum_unit_variant() {
    #[derive(Debug, PartialEq, ArrowField)]
    #[arrow_field(type = "dense")]
    enum TestEnum {
        VAL1,
        VAL2,
        VAL3,
        VAL4,
    }

    let enums = vec![
        TestEnum::VAL1,
        TestEnum::VAL2,
        TestEnum::VAL3,
        TestEnum::VAL4,
    ];
    let b: Box<dyn Array> = enums.try_into_arrow().unwrap();
    let round_trip: Vec<TestEnum> = b.try_into_collection().unwrap();
    assert_eq!(round_trip, enums);
}

#[test]
fn test_sparse_enum_unit_variant() {
    #[derive(Debug, PartialEq, ArrowField)]
    #[arrow_field(type = "sparse")]
    enum TestEnum {
        VAL1,
        VAL2,
        VAL3,
        VAL4,
    }

    let enums = vec![
        TestEnum::VAL1,
        TestEnum::VAL2,
        TestEnum::VAL3,
        TestEnum::VAL4,
    ];
    let b: Box<dyn Array> = enums.try_into_arrow().unwrap();
    let round_trip: Vec<TestEnum> = b.try_into_collection().unwrap();
    assert_eq!(round_trip, enums);
}

#[test]
fn test_nested_unit_variant() {
    #[derive(Debug, PartialEq, ArrowField)]
    struct TestStruct {
        a1: i64,
    }

    #[derive(Debug, PartialEq, ArrowField)]
    #[arrow_field(type = "dense")]
    enum TestEnum {
        VAL1,
        VAL2(i32),
        VAL3(f64),
        VAL4(TestStruct),
        VAL5(ChildEnum),
    }

    #[derive(Debug, PartialEq, ArrowField)]
    #[arrow_field(type = "sparse")]
    enum ChildEnum {
        VAL1,
        VAL2(i32),
        VAL3(f64),
        VAL4(TestStruct),
    }

    let enums = vec![
        TestEnum::VAL1,
        TestEnum::VAL2(2),
        TestEnum::VAL3(1.2),
        TestEnum::VAL4(TestStruct { a1: 10 }),
    ];

    let b: Box<dyn Array> = enums.try_into_arrow().unwrap();
    let round_trip: Vec<TestEnum> = b.try_into_collection().unwrap();
    assert_eq!(round_trip, enums);
}

// TODO: reenable this test once slices for enums is fixed.
//#[test]
#[allow(unused)]
fn test_slice() {
    #[derive(Debug, PartialEq, ArrowField)]
    struct TestStruct {
        a1: i64,
    }

    #[derive(Debug, PartialEq, ArrowField)]
    #[arrow_field(type = "dense")]
    enum TestEnum {
        VAL1,
        VAL2(i32),
        VAL3(f64),
        VAL4(TestStruct),
        VAL5(ChildEnum),
    }

    #[derive(Debug, PartialEq, ArrowField)]
    #[arrow_field(type = "sparse")]
    enum ChildEnum {
        VAL1,
        VAL2(i32),
        VAL3(f64),
        VAL4(TestStruct),
    }

    let enums = vec![
        TestEnum::VAL4(TestStruct { a1: 11 }),
        TestEnum::VAL1,
        TestEnum::VAL2(2),
        TestEnum::VAL3(1.2),
        TestEnum::VAL4(TestStruct { a1: 10 }),
    ];

    let b: Box<dyn Array> = enums.try_into_arrow().unwrap();

    for i in 0..enums.len() {
        let arrow_slice = b.slice(i, enums.len() - i);
        let original_slice = &enums[i..enums.len()];
        let round_trip: Vec<TestEnum> = arrow_slice.try_into_collection().unwrap();
        assert_eq!(round_trip, original_slice);
    }
}
