# arrow2_convert

This crate provides convenience methods on top of [`arrow2`](https://github.com/jorgecarleitao/arrow2) to facilitate conversion between nested rust types and the Arrow memory format.  

The following features are supported:

- arrow2 primitive types can be used as struct fields.
    - numeric types: [`u8`], [`u16`], [`u32`], [`u64`], [`i8`], [`i16`], [`i32`], [`i64`], [`f32`], [`f64`]
    - other types: [`bool`], [`String`], [`Binary`]
    - temporal types: [`chrono::NaiveDate`], [`chrono::NaiveDateTime`]
- Custom types can be used as fields by implementing the ArrowField, ArrowSerialize, and ArrowDeserialize traits.
- Optional fields.
- Deep nesting via structs which derive the `ArrowField` macro or by Vec<T>.

The following are not yet supported. 

- Rust enums, slices, references
- Large lists

Note: This is not an exclusive list. Please see the repo issues for current work in progress and add proposals for features that would be useful for your project.

## API

The API is inspired by serde with the necessary modifications to generate a compile-time Arrow schema and to integrate with arrow2 data structures.

Types (currently only structures) can be annotated with the `ArrowField` procedural macro to derive the following:

- A typed `arrow2::array::MutableArray` for serialization
- A typed iterator for deserialization
- Implementations of the `ArrowField`, `ArrowSerialize`, and `ArrowDeserialize` traits.

Serialization can be performed by using the `arrow_serialize` method or by manually pushing elements to the generated `arrow2::array::MutableArray`.

Deserialization can be performed by using the `arrow_deserialize` method or by iterating through the iterator provided by `arrow_array_deserialize_iterator`.

Both serialization and deserialization perform memory copies for the elements converted. For example, iterating through the deserialize iterator will copy memory from the arrow2 array, into the structure that the iterator returns. Deserialization can be more efficient by supporting structs with references.

## Example

Below is a bare-bones example that does a round trip conversion of a struct with a single field. 

Please see the [complex_example.rs](./arrow2_convert/tests/complex_example.rs) for usage of the full functionality.

```rust
/// Simple example

use arrow2::array::Array;
use arrow2_convert_derive::{ArrowField};
use arrow2_convert::{deserialize::FromArrow,serialize::IntoArrow};

#[derive(Debug, Clone, PartialEq, ArrowField)]
pub struct Foo {
    name: String,
}

#[test]
fn test_simple_roundtrip() {
    // an item
    let original_array = [
        Foo { name: "hello".to_string() },
        Foo { name: "one more".to_string() },
        Foo { name: "good bye".to_string() },
    ];

    // serialize to an arrow array. into_arrow() is enabled by the IntoArrow trait
    let arrow_array: Box<dyn Array> = original_array.into_arrow().unwrap();

    // which can be cast to an Arrow StructArray and be used for all kinds of IPC, FFI, etc.
    // supported by `arrow2`
    let struct_array= arrow_array.as_any().downcast_ref::<arrow2::array::StructArray>().unwrap();
    assert_eq!(struct_array.len(), 3);

    // deserialize back to our original vector via TryIntoIter triat.
    let round_trip_array: Vec<Foo> = arrow_array.try_into_iter().unwrap();
    assert_eq!(round_trip_array, original_array);
}
```

## Implementation details

The goal is to allow the Arrow memory model to be used by an existing rust type tree and to facilitate type conversions, when needed. Ideally, for performance, if the arrow memory model or specifically the API provided by the arrow2 crate exactly matches the custom type tree, then no conversions should be performed.

To achieve this, the following approach is used:

- Introduce three traits, `ArrowField`, `ArrowSerialize`, and `ArrowDeserialize` that can be implemented by types that can be represented in Arrow. Implementations are provided for the built-in arrow2` types, and custom implementations can be provided for other types.

- Blanket implementations are provided for types that recursively contain types that implement the above traits eg. [`Option<T>`], [`Vec<T>`], [`Vec<Option<T>>`], [`Vec<Vec<Option<T>>>`], etc. The blanket implementation needs be enabled by the `arrow_enable_vec_for_type` macro on the primitive type. This explicit enabling is needed since Vec<u8> is a special type in Arrow, and implementation specialization is not yet supported in rust to allow blanket implementations to coexist with more specialized implementations.

- Supporting traits to expose functionality not exposed via arrow2's traits.

### Why not serde?

While serde is the de-facto serialization framework in Rust, it introduces a layer of indirection.
Specifically, arrow2 uses Apache Arrow's in-memory columnar format, while serde is row based. Using serde requires a Serializer/Deserializer implementation around each arrow2 MutableArray and Array, leading to a heavyweight wrapper around simple array manipulations.

Arrow's in-memory format can be serialized/deserialized to a wide variety of formats including Apache Parquet, JSON, Apache Avro, Arrow IPC, and Arrow FFI specification.

One of the objectives of this crate is to derive a compile-time Arrow schema for Rust structs, which we achieve via the `ArrowField` trait.
Other crates that integrate serde for example [serde_arrow](https://github.com/chmp/serde_arrow), 
either need an explicit schema or need to infer the schema at runtime.

Lastly, the serde ecosystem comes with its own default representations for temporal times, that differ from the default representations of arrow2. It seemed best to avoid any conflicts by introducing a new set of traits.

The biggest disadvantage of not using Serde is for types in codebases that already implement serde traits.
They will need to additionally reimplement the traits needed by this crate.
## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
