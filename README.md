# Arrow2-derive - derive for Arrow2

This crate allows writing a struct in Rust and have it derive
a struct of arrays layed out in memory according to the arrow format.

```rust
use arrow2::datatypes::{DataType, Field, TimeUnit};
use arrow2::{array::*, record_batch::RecordBatch};
use arrow2_derive::{IntoArrowStructArray, ArrowStruct};
use chrono::naive::{NaiveDate, NaiveDateTime};
use chrono::Timelike;

#[derive(Debug, Clone, PartialEq, ArrowStruct)]
#[arrow2_derive = "Debug"]
pub struct Foo {
    name: Option<String>,
    is_deleted: bool,
    a1: Option<f64>,
    a2: i64,
    // binary
    a3: Option<Vec<u8>>,
    // date32
    a4: NaiveDate,
    // timestamp(ns, None)
    a5: NaiveDateTime,
    // optional list array of optional strings
    nullable_list: Option<Vec<Option<String>>>,
    // optional list array of required strings
    required_list: Vec<Option<String>>,
    // newtypes, variants or structs that serialize to Arrow primitives can implement the Arrow2Primitive trait
    newtype: CustomDecimal,
}

// Understand how unions are implemented. 
// What are the use cases for performing these transformations?
type CustomDecimal(Vec<u8>);

impl Arrow2Primitive for CustomBinary 
{
    // need arrow primitive type name (should be string?)
    fn type_name() -> &'static str {

    }
    // need conversion function

}

impl Foo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: Option<String>,
        is_deleted: bool,
        a1: Option<f64>,
        a2: i64,
        a3: Option<Vec<u8>>,
        a4: NaiveDate,
        a5: NaiveDateTime,
        nullable_list: Option<Vec<Option<String>>>,
        required_list: Vec<Option<String>>,
    ) -> Self {
        Self {
            name,
            is_deleted,
            a1,
            a2,
            a3,
            a4,
            a5,
            nullable_list,
            required_list,
        }
    }
}

#[test]
fn new() {
    // an item
    let item = Foo::new(
        Some("a".to_string()),
        false,
        Some(0.1),
        1,
        Some(b"aa".to_vec()),
        NaiveDate::from_ymd(1970, 1, 2),
        NaiveDateTime::from_timestamp(10000, 0),
        None,
        vec![Some("aa".to_string()), Some("bb".to_string())],
    );

    let mut array = FooArray::default();
    array.push(item);

    // convert it to an Arrow array
    let array: StructArray = array.into();
    assert_eq!(array.len(), 1);

    // which will have a schema:
    assert_eq!(
        array.fields(),
        &[
            Field::new("name", DataType::Utf8, true),
            Field::new("is_deleted", DataType::Boolean, false),
            Field::new("a1", DataType::Float64, true),
            Field::new("a2", DataType::Int64, false),
            Field::new("a3", DataType::Binary, true),
            Field::new("a4", DataType::Date32, false),
            Field::new("a5", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
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
        ]
    );

    // `StructArray` can then be converted to arrow's `RecordBatch`
    let batch: RecordBatch = array.into();
    assert_eq!(batch.num_columns(), 9);
    assert_eq!(batch.num_rows(), 1);

    // which can be used in IPC, FFI, to parquet, analytics, etc.
}
```

In the example above, the derived struct is

```rust
#[derive(Default, Debug)]
pub struct FooArray {
    name: MutableUtf8Array<i32>,
    is_deleted: MutableBooleanArray<i32>,
    a1: MutablePrimitiveArray<f64>,
    a2: MutablePrimitiveArray<i64>,
    a3: MutableBinaryArray<i32>,
    a4: MutablePrimitiveArray<i32>,
    a5: MutablePrimitiveArray<i64>,
    nullable_list: MutableListArray<i32, MutableUtf8Array<i32>>,
    required_list: MutableListArray<i32, MutableUtf8Array<i32>>,
    other_list: MutableListArray<i32, MutablePrimitiveArray<i32>>,
}
```

`FooArray::push` lays data in memory according to the arrow spec and
can be used for all kinds of IPC, FFI, etc. supported by `arrow2`.

## Implementation details

The goal is to allow the Arrow memory model to be used by an existing rust type tree and to facilitate conversions, when needed. Ideally, for performance, if the arrow memory model or specifically the API provided by the arrow2 crate exactly matches the custom type tree, then no conversions should be performed.

To achieve this, the following approach is used. 

- Introduce three traits, `ArrowField`, `ArrowSerialize`, and `ArrowDeserialize` that can be implemented by types that can be represented in Arrow. Implementations are provided for the built-in arrow2` types, and custom implementations can be provided for other types.

- Blanket implementations are provided for types that recursively contain types that implement the above traits eg. [`Option<T>`], [`Vec<T>`], [`Vec<Option<T>>`], [`Vec<Vec<Option<T>>>`], etc. The blanked implementation needs be enabled by the `arrow_enable_vec_for_type` macro on the primitive type.

- The supporting trait [`crate::ArrowMutableArray`] is used for generic manipulation of [`arrow2::array::MutableArray`] implementations.. This trait is also implemented for mutable struct arrays generated by the derive macro.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
