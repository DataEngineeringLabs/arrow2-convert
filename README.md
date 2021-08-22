# Arrow2-derive - derive for Arrow2

This crate allows

```rust
use arrow2::datatypes::{DataType, Field};
use arrow2::{array::*, record_batch::RecordBatch};
use arrow2_derive::{ArrowStruct, StructOfArrow};

#[derive(Debug, Clone, PartialEq, StructOfArrow)]
#[arrow2_derive = "Default, Debug"]
pub struct Foo {
    name: Option<String>,
    mass: Option<f64>,
    mass1: i64,
    // binary
    a3: Option<Vec<u8>>,
    // optional list array of optional strings
    nullable_list: Option<Vec<Option<String>>>,
    // optional list array of required strings
    required_list: Vec<Option<String>>,
    // other
    other_list: Option<Vec<Option<i32>>>,
}

fn main() {
    // create and populate
    let mut a = FooArray::default();
    a.push(
        Some("a"),
        Some(0.1),
        1,
        Some(b"aa"),
        None,
        vec![Some("aa"), Some("bb")],
        None,
    );
    a.push(
        Some("a"),
        Some(0.2),
        2,
        None,
        Some(vec![Some("aa"), Some("bb")]),
        vec![Some("aa"), Some("bb")],
        Some(vec![Some(1), Some(2)]),
    );

    // convert it to an Arrow array
    let array: StructArray = a.into();
    assert_eq!(array.len(), 2);

    // which will have a schema:
    assert_eq!(
        array.fields(),
        &[
            Field::new("name", DataType::Utf8, true),
            Field::new("mass", DataType::Float64, true),
            Field::new("mass1", DataType::Int64, false),
            Field::new("a3", DataType::Binary, true),
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
                "other_list",
                DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
                false
            ),
        ]
    );

    // `StructArray` can then be converted to arrow's `RecordBatch`
    let batch: RecordBatch = array.into();
    assert_eq!(batch.num_columns(), 6);
    assert_eq!(batch.num_rows(), 2);

    // which can be used in IPC, FFI, analytics, etc.
}
```

thereby allowing users to write struct of arrays idiomatically in Rust and
have them be layed out in memory according to the arrow format.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
