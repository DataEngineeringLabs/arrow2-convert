/// Complex example of the derive API. Includes the following features
/// - Nested structs and lists
/// - Custom types

use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use arrow2_derive::{ArrowField,ArrowStruct,ArrowSerialize, ArrowDeserialize};

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
    vec_custom: Vec<CustomType>,
    // nested struct
    child: Child,
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
    a1: i32
}

impl Root {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: Option<String>,
        is_deleted: bool,
        a1: Option<f64>,
        a2: i64,
        a3: Option<Vec<u8>>,
        a4: chrono::NaiveDate,
        a5: chrono::NaiveDateTime,
        a6: Option<chrono::NaiveDateTime>,
        date_time_list: Vec<chrono::NaiveDateTime>,
        nullable_list: Option<Vec<Option<String>>>,
        required_list: Vec<Option<String>>,
        custom: CustomType,
        nullable_custom: Option<CustomType>,
        vec_custom: Vec<CustomType>,
        //custom_list: Vec<CustomType>,
        child: Child,
    ) -> Self {
        Self {
            name,
            is_deleted,
            a1,
            a2,
            a3,
            a4,
            a5,
            a6,
            date_time_list,
            nullable_list,
            required_list,
            custom,
            nullable_custom,
            vec_custom,
            child
        }
    }
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
    type MutableArrayType = MutablePrimitiveArray<u64>;
    type SerializeOutput = u64;

    #[inline]
    fn arrow_serialize(v: Option<Self>) -> Option<u64> {
        v.map(|t| t.0)
    }
}

impl ArrowDeserialize for CustomType {
    type ArrayType = PrimitiveArray<u64>;

    #[inline]
    fn arrow_deserialize<'a>(v: Option<&'a u64>) -> Option<Self> {
        v.map(|t|CustomType(*t))
    }
}

// Enable Vec<CustomType>
arrow2_derive::arrow_enable_vec_for_type!(CustomType);


#[test]
fn test_derive() {
    use arrow2::array::*;
    use arrow2::datatypes::{DataType,Field,TimeUnit};
    use chrono::{NaiveDate, NaiveDateTime};

    // an item
    let item = Root::new(
        Some("a".to_string()),
        false,
        Some(0.1),
        1,
        Some(b"aa".to_vec()),
        NaiveDate::from_ymd(1970, 1, 2),
        NaiveDateTime::from_timestamp(10000, 0),
        Some(NaiveDateTime::from_timestamp(10001, 0)),
        vec![NaiveDateTime::from_timestamp(10000, 10), NaiveDateTime::from_timestamp(10000, 11)],
        Some(vec![Some("cc".to_string()), Some("dd".to_string())]),
        vec![Some("aa".to_string()), Some("bb".to_string())],
        CustomType(10),
        Some(CustomType(11)),
        vec![CustomType(12), CustomType(13)],
        Child {
            a1: 10,
            a2: "hello".to_string(),
            child_array: vec![
                ChildChild { a1: 100 },
                ChildChild { a1: 101 },
            ]
        }
    );

    // another item
    let item1 = Root::new(
        Some("b".to_string()),
        true,
        Some(0.1),
        1,
        Some(b"aa".to_vec()),
        NaiveDate::from_ymd(1970, 1, 2),
        NaiveDateTime::from_timestamp(10000, 0),
        None,
        vec![NaiveDateTime::from_timestamp(10000, 13), NaiveDateTime::from_timestamp(10000, 14)],
        None,
        vec![Some("ee".to_string()), Some("ff".to_string())],
        CustomType(11),
        None,
        vec![CustomType(14), CustomType(13)],
        Child {
            a1: 11,
            a2: "hello again".to_string(),
            child_array: vec![
                ChildChild { a1: 100 },
                ChildChild { a1: 102 },
            ]
        }
    );

    // serialize to an arrow array
    let array = arrow2_derive::arrow_serialize(vec![item.clone(), item1.clone()]).unwrap();
    let struct_array= array.as_any().downcast_ref::<arrow2::array::StructArray>().unwrap();
    assert_eq!(struct_array.len(), 2);

    let values = struct_array.values();
    assert_eq!(values.len(), 15);
    assert_eq!(struct_array.len(), 2);

    // which can be used in IPC, FFI, to parquet, analytics, etc.
    use std::ops::Deref;
    // deserialize back to our original vector
    let foo_array: Vec<Root> = arrow2_derive::arrow_deserialize(array.deref()).unwrap();
    assert_eq!(foo_array, vec![item.clone(), item1.clone()]);

    // check explicit schema
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
                "vec_custom",
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
                                            vec![Field::new("a1", DataType::Int32, false)]
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
        ]
    );
}
