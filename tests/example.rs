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

impl Foo {
    pub fn new(
        name: Option<String>,
        mass: Option<f64>,
        mass1: i64,
        a3: Option<Vec<u8>>,
        nullable_list: Option<Vec<Option<String>>>,
        required_list: Vec<Option<String>>,
        other_list: Option<Vec<Option<i32>>>,
    ) -> Self {
        Self {
            name,
            mass,
            mass1,
            a3,
            nullable_list,
            required_list,
            other_list,
        }
    }
}

#[test]
fn new() {
    // an item
    let item = Foo::new(
        Some("a".to_string()),
        Some(0.1),
        1,
        Some(b"aa".to_vec()),
        None,
        vec![Some("aa".to_string()), Some("bb".to_string())],
        None,
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
                true
            ),
        ]
    );

    // `StructArray` can then be converted to arrow's `RecordBatch`
    let batch: RecordBatch = array.into();
    assert_eq!(batch.num_columns(), 7);
    assert_eq!(batch.num_rows(), 1);

    // which can be used in IPC, FFI, analytics, etc.
}
