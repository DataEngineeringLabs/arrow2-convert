/// Tests that the macro generated code doesn't assume the presence of additional bindings and uses absolute paths
use arrow2_convert_derive::ArrowField;

#[derive(ArrowField)]
struct S {
    int_field: i64,
}
