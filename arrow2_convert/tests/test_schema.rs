use std::sync::Arc;

use arrow2::datatypes::*;
use arrow2_convert::ArrowField;

#[test]
fn test_schema_types() {
    #[derive(Debug, ArrowField)]
    #[allow(dead_code)]
    struct Root {
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
        // i128(precision, scale)
        #[arrow_field(type = "arrow2_convert::field::I128<32, 32>")]
        a7: i128,
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
        // large binary
        #[arrow_field(type = "arrow2_convert::field::LargeBinary")]
        large_binary: Vec<u8>,
        // fixed size binary
        #[arrow_field(type = "arrow2_convert::field::FixedSizeBinary<3>")]
        fixed_size_binary: Vec<u8>,
        // large string
        #[arrow_field(type = "arrow2_convert::field::LargeString")]
        large_string: String,
        // large vec
        #[arrow_field(type = "arrow2_convert::field::LargeVec<i64>")]
        large_vec: Vec<i64>,
        // fixed size vec
        #[arrow_field(type = "arrow2_convert::field::FixedSizeVec<i64, 3>")]
        fixed_size_vec: Vec<i64>,
    }

    #[derive(Debug, ArrowField)]
    #[allow(dead_code)]
    struct Child {
        a1: i64,
        a2: String,
        // nested struct array
        child_array: Vec<ChildChild>,
    }

    #[derive(Debug, ArrowField)]
    #[allow(dead_code)]
    pub struct ChildChild {
        a1: i32,
        bool_array: Vec<bool>,
        int64_array: Vec<i64>,
    }

    // enable Vec<CustomType>
    arrow2_convert::arrow_enable_vec_for_type!(CustomType);

    #[derive(Debug)]
    /// A newtype around a u64
    pub struct CustomType(u64);

    impl arrow2_convert::field::ArrowField for CustomType {
        type Type = Self;

        fn data_type() -> arrow2::datatypes::DataType {
            arrow2::datatypes::DataType::Extension(
                "custom".to_string(),
                Arc::new(arrow2::datatypes::DataType::UInt64),
                None,
            )
        }
    }

    impl arrow2_convert::serialize::ArrowSerialize for CustomType {
        type MutableArrayType = arrow2::array::MutablePrimitiveArray<u64>;

        #[inline]
        fn new_array() -> Self::MutableArrayType {
            unimplemented!();
        }

        #[inline]
        fn arrow_serialize(
            _v: &Self,
            _array: &mut Self::MutableArrayType,
        ) -> arrow2::error::Result<()> {
            unimplemented!();
        }
    }

    impl arrow2_convert::deserialize::ArrowDeserialize for CustomType {
        type ArrayType = arrow2::array::PrimitiveArray<u64>;

        #[inline]
        fn arrow_deserialize(_v: Option<&u64>) -> Option<Self> {
            unimplemented!();
        }
    }

    assert_eq!(
        <Root as arrow2_convert::field::ArrowField>::data_type(),
        DataType::Struct(Arc::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("is_deleted", DataType::Boolean, false),
            Field::new("a1", DataType::Float64, true),
            Field::new("a2", DataType::Int64, false),
            Field::new("a3", DataType::Binary, true),
            Field::new("a4", DataType::Date32, false),
            Field::new("a5", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("a6", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("a7", DataType::Decimal(32, 32), false),
            Field::new(
                "date_time_list",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false
                ))),
                false
            ),
            Field::new(
                "nullable_list",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true
            ),
            Field::new(
                "required_list",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false
            ),
            Field::new(
                "custom",
                DataType::Extension("custom".to_string(), Arc::new(DataType::UInt64), None),
                false
            ),
            Field::new(
                "nullable_custom",
                DataType::Extension("custom".to_string(), Arc::new(DataType::UInt64), None),
                true
            ),
            Field::new(
                "custom_list",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Extension("custom".to_string(), Arc::new(DataType::UInt64), None),
                    false
                ))),
                false
            ),
            Field::new(
                "child",
                DataType::Struct(Arc::new(vec![
                    Field::new("a1", DataType::Int64, false),
                    Field::new("a2", DataType::Utf8, false),
                    Field::new(
                        "child_array",
                        DataType::List(Arc::new(Field::new(
                            "item",
                            DataType::Struct(Arc::new(vec![
                                Field::new("a1", DataType::Int32, false),
                                Field::new(
                                    "bool_array",
                                    DataType::List(Arc::new(Field::new(
                                        "item",
                                        DataType::Boolean,
                                        false
                                    ))),
                                    false
                                ),
                                Field::new(
                                    "int64_array",
                                    DataType::List(Arc::new(Field::new(
                                        "item",
                                        DataType::Int64,
                                        false
                                    ))),
                                    false
                                ),
                            ])),
                            false
                        ))),
                        false
                    )
                ])),
                false
            ),
            Field::new(
                "int32_array",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false
            ),
            Field::new("large_binary", DataType::LargeBinary, false),
            Field::new("fixed_size_binary", DataType::FixedSizeBinary(3), false),
            Field::new("large_string", DataType::LargeUtf8, false),
            Field::new(
                "large_vec",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, false))),
                false
            ),
            Field::new(
                "fixed_size_vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, false)), 3),
                false
            ),
        ]))
    );
}

#[test]
fn test_large_string_schema() {
    use arrow2_convert::field::LargeString;

    assert_eq!(
        <LargeString as arrow2_convert::field::ArrowField>::data_type(),
        DataType::LargeUtf8
    );
    assert!(!<LargeString as arrow2_convert::field::ArrowField>::is_nullable());
    assert!(<Option<LargeString> as arrow2_convert::field::ArrowField>::is_nullable());

    assert_eq!(
        <Vec<LargeString> as arrow2_convert::field::ArrowField>::data_type(),
        DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, false)))
    );
}
