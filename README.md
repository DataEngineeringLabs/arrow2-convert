# Arrow2-derive - derive for Arrow2

This crate enables converting between arrays of rust structures and the Arrow memory format as represented by arrow2 data structures. Specifically, it exposes a `ArrowStruct` derive macro, which can be used to annotate a structure to derive the following:
- A arrow2::array::MutableArray, which is used for serialization, and converted to an arrow2::array::Array for use by the the rest of the Arrow/arrow2 ecosystem.
- A typed arrow2::array::Array which can be used to deserialize back to the original array.

The following features are supported:

- arrow2 primitive types can be used as struct fields.
    - numeric types: [`u8`], [`u16`], [`u32`], [`u64`], [`i8`], [`i16`], [`i32`], [`i64`], [`f32`], [`f64`]
    - other types: [`bool`], [`String`]
    - temporal types: [`chrono::NaiveDate`], [`chrono::NaiveDateTime`]
- Custom types can be used as fields by implementing the ArrowField, ArrowSerialize, and ArrowDeserialize traits.
- Optional fields: Option<T>.
- Deep nesting via nested structs, which derive the `ArrowStruct` macro or by Vec<T>.

The following features are not yet supported. 

- Nested optional structs Vec<Option<T>>
- Rust enums, slices, references

Note: This is not an exclusive list. Please see the repo issues for current work in progress. Please also feel free to add proposals for features that would be useful for your project.
## Usage

Below is a bare-bones example that does a round trip conversion of a struct with a single field. 

Please see the [complex_example.rs](./tests/complex_example.rs) for usage of the full functionality.

```rust
use arrow2::datatypes::{DataType, Field, TimeUnit};
use arrow2::array::Array;
use arrow2_derive::{ArrowStruct,FromArrow,IntoArrow};

// Annotate the struct with ArrowStruct
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

    // serialize to an arrow array. into_arrow() is enabled by the IntoArrow trait
    let arrow_array: Box<dyn Array> = original_array.clone().into_arrow().unwrap();

    // which can be cast to an Arrow StructArray and be used for all kinds of IPC, FFI, etc. 
    // supported by `arrow2`
    let struct_array= arrow_array.as_any().downcast_ref::<arrow2::array::StructArray>().unwrap();
    assert_eq!(struct_array.len(), 3);

    // deserialize back to our original vector. from_arrow() is enabled by the FromArrow trait
    let round_trip_array: Vec<Foo> = arrow_array.from_arrow().unwrap();
    assert_eq!(round_trip_array, original_array);
}
```

## Implementation details

The goal is to allow the Arrow memory model to be used by an existing rust type tree and to facilitate type conversions, when needed. Ideally, for performance, if the arrow memory model or specifically the API provided by the arrow2 crate exactly matches the custom type tree, then no conversions should be performed.

To achieve this, the following approach is used. 

- Introduce three traits, `ArrowField`, `ArrowSerialize`, and `ArrowDeserialize` that can be implemented by types that can be represented in Arrow. Implementations are provided for the built-in arrow2` types, and custom implementations can be provided for other types.

- Blanket implementations are provided for types that recursively contain types that implement the above traits eg. [`Option<T>`], [`Vec<T>`], [`Vec<Option<T>>`], [`Vec<Vec<Option<T>>>`], etc. The blanket implementation needs be enabled by the `arrow_enable_vec_for_type` macro on the primitive type. This explicit enabling is needed since Vec<u8> is a special type in Arrow, and implementation specialization is not yet supported in rust to allow blanket implementations to coexist with more specialized implementations.

- Some additional supporting traits such as `ArrowMutableArrayTryPushGeneric` are used to workaround the unavailability of features like generic associated types.

### Why not serde?

While, serde is the de-facto serialization framework for rust it introduces a layer of indirection that seemed unnecessary for the arrow2 framework. Specifically, arrow2 itself serves as an intermediate representation, which can be serialized/deserialized to a wide variety of formats including JSON, avro, etc, so it seemed conceptually more straightforward to have the derive functionality to have much tighter coupling with arrow2.

Also, one of the objectives of this crate is to derive a compile-time schema for rust structs, which requires an orthogonal approach to serde. This is achieved by the `ArrowField` trait. Other crates that integrate serde for example [serde_arrow](https://github.com/chmp/serde_arrow), either need an explicit schema or need to infer the schema at runtime.

Lastly, the serde ecosystem comes with its own default representations for temporal times, that differ from the default representations of arrow2. It seemed best to avoid any conflicts by introducing a new set of traits.

Probably, the biggest disadvantage of not using Serde is types in codebases which already implement serde traits, will need to additionally reimplement the traits needed by this crate. If this starts to become an issue, then we can look into introducing a serde adapter to leverage those implementations.
## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
