use arrow2::array::*;
use arrow2::datatypes::Field;
use arrow2_derive::{ArrowStruct, StructOfArrow};

#[derive(Debug, Clone, PartialEq, StructOfArrow)]
#[arrow2_derive = "Default, Debug"]
pub struct Foo {
    name: String,
    mass: f64,
}

#[test]
fn new() {
    let mut a = FooArray::new();
    a.push(Some("a"), Some(0.1));

    let array = a.to_arrow();
    assert_eq!(array.len(), 1);
}
