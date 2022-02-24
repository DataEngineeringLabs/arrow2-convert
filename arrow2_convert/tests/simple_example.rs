/// Simple example
use arrow2::array::Array;
use arrow2_convert::{deserialize::TryIntoIter, serialize::IntoArrow, ArrowField};

#[derive(Debug, Clone, PartialEq, ArrowField)]
pub struct Foo {
    name: String,
}

#[test]
fn test_simple_roundtrip() {
    // an item
    let original_array = [
        Foo {
            name: "hello".to_string(),
        },
        Foo {
            name: "one more".to_string(),
        },
        Foo {
            name: "good bye".to_string(),
        },
    ];

    // serialize to an arrow array. into_arrow() is enabled by the IntoArrow trait
    let arrow_array: Box<dyn Array> = original_array.into_arrow().unwrap();

    // which can be cast to an Arrow StructArray and be used for all kinds of IPC, FFI, etc.
    // supported by `arrow2`
    let struct_array = arrow_array
        .as_any()
        .downcast_ref::<arrow2::array::StructArray>()
        .unwrap();
    assert_eq!(struct_array.len(), 3);

    // deserialize back to our original vector via TryIntoIter trait.
    let round_trip_array: Vec<Foo> = arrow_array.try_into_iter().unwrap();
    assert_eq!(round_trip_array, original_array);
}
