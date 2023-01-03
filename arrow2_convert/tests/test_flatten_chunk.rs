use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2_convert::{serialize::*, ArrowField, ArrowSerialize};
use std::sync::Arc;

#[test]
fn test_flatten_chunk() {
    #[derive(Debug, Clone, ArrowField, ArrowSerialize)]
    struct Struct {
        a: i64,
        b: i64,
    }

    let target = Chunk::new(vec![
        Int64Array::from(&[Some(1), Some(2)]).boxed(),
        Int64Array::from(&[Some(1), Some(2)]).boxed(),
    ]);

    let array = vec![Struct { a: 1, b: 1 }, Struct { a: 2, b: 2 }];

    let array: Box<dyn Array> = array.try_into_arrow().unwrap();
    let chunk: Chunk<Box<dyn Array>> = Chunk::new(vec![array]);

    let flattened: Chunk<Box<dyn Array>> = chunk.flatten().unwrap();

    assert_eq!(flattened, target);
}

#[test]
fn test_flatten_chunk_empty_chunk_error() {
    let chunk: Chunk<Arc<dyn Array>> = Chunk::new(vec![]);
    assert!(chunk.flatten().is_err());
}

#[test]
fn test_flatten_chunk_no_single_struct_array_error() {
    #[derive(Debug, Clone, ArrowField, ArrowSerialize)]
    struct Struct {
        a: i64,
        b: String,
    }

    let array = vec![
        Struct {
            a: 1,
            b: "one".to_string(),
        },
        Struct {
            a: 2,
            b: "two".to_string(),
        },
    ];

    let array: Arc<dyn Array> = array.try_into_arrow().unwrap();

    let arrays = vec![array.clone(), array.clone()];
    let chunk = Chunk::new(arrays);

    assert!(chunk.flatten().is_err());
}

#[test]
fn test_flatten_chunk_type_not_struct_error() {
    let array: Arc<dyn Array> = Int32Array::from(&[Some(1), None, Some(3)]).arced();
    let chunk = Chunk::new(vec![array]);

    assert!(chunk.flatten().is_err());
}
