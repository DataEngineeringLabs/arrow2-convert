use arrow2::datatypes::{DataType, Field};
use arrow2::{array::*, record_batch::RecordBatch};
use arrow2_derive::{ArrowStruct, StructOfArrow};

#[derive(Debug, Clone, PartialEq, StructOfArrow)]
#[arrow2_derive = "Default, Debug"]
pub struct LogData {
    time: String,
    query: String,
    user: String,
    db: String,
    pid: i32,
    xid: i32,
    uid: i32,
}

impl LogData {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        time: String,
        query: String,
        user: String,
        db: String,
        pid: i32,
        xid: i32,
        uid: i32,
    ) -> Self {
        Self {
            time,
            query,
            user,
            db,
            pid,
            xid,
            uid,
        }
    }
}

#[test]
fn main() {
    // an item
    let item = LogData::new(
        "a".to_string(),
        "a".to_string(),
        "a".to_string(),
        "a".to_string(),
        32,
        32,
        32,
    );

    let mut array = LogDataArray::default();
    array.push(item);

    // convert it to an Arrow array
    let array: StructArray = array.into();
    assert_eq!(array.len(), 1);

    // // which will have a schema:
    // assert_eq!(
    //     array.fields(),
    //     &[
    //         Field::new("name", DataType::Utf8, true),
    //         Field::new("is_deleted", DataType::Boolean, false),
    //         Field::new("a1", DataType::Float64, true),
    //         Field::new("a2", DataType::Int64, false),
    //         Field::new("a3", DataType::Binary, true),
    //         Field::new(
    //             "nullable_list",
    //             DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
    //             true
    //         ),
    //         Field::new(
    //             "required_list",
    //             DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
    //             false
    //         ),
    //         Field::new(
    //             "other_list",
    //             DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
    //             true
    //         ),
    //     ]
    // );

    // `StructArray` can then be converted to arrow's `RecordBatch`
    let batch: RecordBatch = array.into();
    assert_eq!(batch.num_columns(), 7);
    assert_eq!(batch.num_rows(), 1);

    // which can be used in IPC, FFI, to parquet, analytics, etc.
}
