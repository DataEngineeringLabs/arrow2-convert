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
}

#[test]
fn new() {
    // create and populate
    let mut a = FooArray::default();
    a.push(Some("a"), Some(0.1), 1, Some(b"aa"));
    a.push(Some("a"), Some(0.2), 2, None);

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
        ]
    );

    // structs can be converted to arrow's `RecordBatch`
    let batch: RecordBatch = (&array).into();
    assert_eq!(batch.num_columns(), 4);
    assert_eq!(batch.num_rows(), 2);

    // which can be used in IPC, FFI, analytics, etc.
}
