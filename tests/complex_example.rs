/// Complex example of the derive API. Includes the following features
/// 
/// - Nested structs and lists
/// - Custom types
/// 
/// Note:
/// 
/// We restrict the top-level bindings to arrow2_derive to test that the derive macro does
/// not rely on other bindings and to ensure that it uses absolute paths to refer to other 
/// types.
use arrow2_derive::{ArrowField,ArrowStruct,ArrowSerialize,ArrowDeserialize,FromArrow,IntoArrow};

#[derive(Debug, Clone, PartialEq, ArrowStruct)]
#[arrow2_derive = "Debug"]
pub struct Root {
    name: Option<String>,
    is_deleted: bool,
    a1: Option<f64>,
    a2: i64,
    // binary
    a3: Option<Vec<u8>>,
    // date32
    a4: chrono::NaiveDate,
    // timestamp(ns, None)
    a5: chrono::NaiveDateTime,
    // timestamp(ns, None)
    a6: Option<chrono::NaiveDateTime>,
    // array of date times
    date_time_list: Vec<chrono::NaiveDateTime>,
    // optional list array of optional strings
    nullable_list: Option<Vec<Option<String>>>,
    // optional list array of required strings
    required_list: Vec<Option<String>>,
    // custom type
    custom: CustomType,
    // custom optional type
    nullable_custom: Option<CustomType>,
    // vec custom type
    custom_list: Vec<CustomType>,
    // nested struct
    child: Child,
    // int 32 array
    int32_array: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, ArrowStruct)]
#[arrow2_derive = "Debug"]
pub struct Child {
    a1: i64,
    a2: String,
    // nested struct array
    child_array: Vec<ChildChild>
}

#[derive(Debug, Clone, PartialEq, ArrowStruct)]
#[arrow2_derive = "Debug"]
pub struct ChildChild {
    a1: i32,
    bool_array: Vec<bool>,
    int64_array: Vec<i64>
}

#[derive(Debug, Clone, PartialEq)]
/// A newtype around a u64
pub struct CustomType(u64);

/// To use with Arrow three traits need to be implemented:
/// - ArrowField
/// - ArrowSerialize
/// - ArrowDeserialize
impl ArrowField for CustomType
{
    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Extension("custom".to_string(), Box::new(arrow2::datatypes::DataType::UInt64), None)
    }
}

impl ArrowSerialize for CustomType {
    type MutableArrayType = arrow2::array::MutablePrimitiveArray<u64>;
    type SerializeOutput = u64;

    #[inline]
    fn arrow_serialize(v: Option<Self>) -> Option<u64> {
        v.map(|t| t.0)
    }
}

impl ArrowDeserialize for CustomType {
    type ArrayType = arrow2::array::PrimitiveArray<u64>;

    #[inline]
    fn arrow_deserialize<'a>(v: Option<&'a u64>) -> Option<Self> {
        v.map(|t|CustomType(*t))
    }
}

// enable Vec<CustomType>
arrow2_derive::arrow_enable_vec_for_type!(CustomType);

fn item1() -> Root {
    use chrono::{NaiveDate,NaiveDateTime};

    Root {
        name: Some("a".to_string()),
        is_deleted: false,
        a1: Some(0.1),
        a2: 1,
        a3: Some(b"aa".to_vec()),
        a4: NaiveDate::from_ymd(1970, 1, 2),
        a5: NaiveDateTime::from_timestamp(10000, 0),
        a6: Some(NaiveDateTime::from_timestamp(10001, 0)),
        date_time_list: vec![NaiveDateTime::from_timestamp(10000, 10), NaiveDateTime::from_timestamp(10000, 11)],
        nullable_list: Some(vec![Some("cc".to_string()), Some("dd".to_string())]),
        required_list: vec![Some("aa".to_string()), Some("bb".to_string())],
        custom: CustomType(10),
        nullable_custom: Some(CustomType(11)),
        custom_list: vec![CustomType(12), CustomType(13)],
        child: Child {
            a1: 10,
            a2: "hello".to_string(),
            child_array: vec![
                ChildChild { 
                    a1: 100,
                    bool_array: vec![false],
                    int64_array: vec![45555, 2124214, 224, 24214, 2424]
                },
                ChildChild { 
                    a1: 101,
                    bool_array: vec![true, true, true],
                    int64_array: vec![4533, 22222, 2323, 333, 33322]
                },
            ]
        },
        int32_array: vec![ 0, 1, 3 ]
    }
}

fn item2() -> Root {
    use chrono::{NaiveDate, NaiveDateTime};

    Root {
        name: Some("b".to_string()),
        is_deleted: true,
        a1: Some(0.1),
        a2: 1,
        a3: Some(b"aa".to_vec()),
        a4: NaiveDate::from_ymd(1970, 1, 2),
        a5: NaiveDateTime::from_timestamp(10000, 0),
        a6: None,
        date_time_list: vec![NaiveDateTime::from_timestamp(10000, 10), NaiveDateTime::from_timestamp(10000, 11)],
        nullable_list: None,
        required_list: vec![Some("ee".to_string()), Some("ff".to_string())],
        custom: CustomType(11),
        nullable_custom: None,
        custom_list: vec![CustomType(14), CustomType(13)],
        child: Child {
            a1: 11,
            a2: "hello again".to_string(),
            child_array: vec![
                ChildChild { 
                    a1: 100,
                    bool_array: vec![true, false, false, true],
                    int64_array: vec![111111, 2222, 33]
                },
                ChildChild { 
                    a1: 102,
                    bool_array: vec![false],
                    int64_array: vec![45555, 2124214, 224, 24214, 2424]
                },
            ]
        },
        int32_array: vec![ 111, 1 ]
    }
}

#[test]
fn test_round_trip() {
    use arrow2::array::*;

    // serialize to an arrow array
    let original_array = vec![item1(), item2()];

    let array: Box<dyn Array> = original_array.clone().into_arrow().unwrap();
    let struct_array= array.as_any().downcast_ref::<arrow2::array::StructArray>().unwrap();
    assert_eq!(struct_array.len(), 2);

    let values = struct_array.values();
    assert_eq!(values.len(), 16);
    assert_eq!(struct_array.len(), 2);

    // deserialize back to our original vector
    let foo_array: Vec<Root> = array.from_arrow().unwrap();
    assert_eq!(foo_array, original_array);
}


#[test]
fn test_schema()
{
    use arrow2::datatypes::*;
    use arrow2::array::*;

    let original_array = vec![item1()];
    let array: Box<dyn Array> = original_array.clone().into_arrow().unwrap();
    let struct_array= array.as_any().downcast_ref::<arrow2::array::StructArray>().unwrap();

    assert_eq!(
        struct_array.fields(),
        &[
            Field::new("name", DataType::Utf8, true),
            Field::new("is_deleted", DataType::Boolean, false),
            Field::new("a1", DataType::Float64, true),
            Field::new("a2", DataType::Int64, false),
            Field::new("a3", DataType::Binary, true),
            Field::new("a4", DataType::Date32, false),
            Field::new("a5", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("a6", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "date_time_list",
                DataType::List(Box::new(Field::new("item", DataType::Timestamp(TimeUnit::Nanosecond, None), false))),
                false
            ),
            Field::new(
                "nullable_list",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true
            ),
            Field::new(
                "required_list",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                false
            ),
            Field::new(
                "custom",
                DataType::Extension("custom".to_string(), Box::new(DataType::UInt64), None),
                false
            ),
            Field::new(
                "nullable_custom",
                DataType::Extension("custom".to_string(), Box::new(DataType::UInt64), None),
                true
            ),
            Field::new(
                "custom_list",
                DataType::List(Box::new(Field::new("item", DataType::Extension("custom".to_string(), Box::new(DataType::UInt64), None), false))),
                false
            ),
            Field::new(
                "child",
                DataType::Struct(
                    vec![
                        Field::new("a1", DataType::Int64, false),
                        Field::new("a2", DataType::Utf8, false),
                        Field::new(
                            "child_array",
                            DataType::List(
                                Box::new(
                                    Field::new(
                                        "item",
                                        DataType::Struct(
                                            vec![
                                                Field::new("a1", DataType::Int32, false),
                                                Field::new(
                                                    "bool_array",
                                                    DataType::List(Box::new(Field::new("item", DataType::Boolean, false))),
                                                    false
                                                ),
                                                Field::new(
                                                    "int64_array",
                                                    DataType::List(Box::new(Field::new("item", DataType::Int64, false))),
                                                    false
                                                ),                                    
                                            ]
                                        ),
                                        false
                                    )
                                )
                            ),
                            false
                        )
                    ]
                ),
                false
            ),
            Field::new(
                "int32_array",
                DataType::List(Box::new(Field::new("item", DataType::Int32, false))),
                false
            )
        ]
    );
}