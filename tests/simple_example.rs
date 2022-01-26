/// Simple example of the derive API

use arrow2::array::Array;
use arrow2_derive::ArrowStruct;

#[derive(Debug, Clone, PartialEq, ArrowStruct)]
#[arrow2_derive = "Debug"]
pub struct Foo {
    name: String,
}

#[test]
fn test() {
    // an item
    let original_array = vec![
        Foo { name: "hello".to_string() },
        Foo { name: "one more".to_string() },
        Foo { name: "good bye".to_string() },
    ];

    // serialize to an arrow array
    let arrow_array = arrow2_derive::arrow_serialize(original_array.clone()).unwrap();

    // which can be cast to an Arrow StructArray
    let struct_array= arrow_array.as_any().downcast_ref::<arrow2::array::StructArray>().unwrap();
    assert_eq!(struct_array.len(), 3);

    use std::ops::Deref;
    // deserialize back to our original vector
    let round_trip_array: Vec<Foo> = arrow2_derive::arrow_deserialize(arrow_array.deref()).unwrap();
    assert_eq!(round_trip_array, original_array);
}
