deserialize:
- source: a physical arrow type
- target: any rust type

serialize:
  - source: any rust type
  - target: a physical arrow type

These naturally correspond to conversion methods that any type that aspires to be an arrow field can implement.

For non-list fields:
  - deserialize conversion can look like:
    ```rust
    deserialize(source: ArrowType) -> Type
    ```
  - serialize conversion can look like:
    ```rust
    serialize(source: Type) -> ArrowType
    ```

lists are tricky:
- lists in arrow are represented as offsets into a physical array. nested lists are simply layers of offsets.
- the physical types of lists are iterators and since we cannot return `impl` types from traits yet, representing the types is cumbersome. Fortunately, now that GATs are nearly stable we can use those for deserialize. We'll need to hack serialize for now (more on this below).
 
For list fields:
- deserialize conversion can look like:
    ```rust
    type Source = Iterator<Element=ArrowType>;
    type Target = Collection;
    deserialize<S>(source: Source) -> Target
    ```

We need connect this conversion method to the data-structures arrow2 uses. Specifically, we need to get the iterator for an arrow2::array::Array. But which data structures need to be used for a specific-field? We need to map these with an associated type.

This leads to the following deserialize trait:

```rust
trait ArrowDeserialize {
    type Source;
    type Target;
    type Array: ArrayAdapter 
    fn deserialize<S>(source: Source) -> Target;
}
```

A further simplification can be made since we can infer types if we design the Adapter traits correctly, and also since both of these traits inherit from the `ArrowField` trait which defines the rust type:

```rust
trait ArrowDeserialize {
    // can be inferred from ArrayAdapter
    //type Source;
    // can be inferred from ArrowField
    // type Target;
    type Array: ArrayAdapter 
    fn deserialize<S>(source: Source) -> Target;
}
```

Why do we need an explicit `Type` why not `Self`? Two reasons:

1. To support field overrides, which in-turn support two use cases:
   1. Custom conversion methods for a specific field 
   2. Allow using i64 memory offsets for larger data.
2. To use the same set of traits for collections, so that we can support collections without explicit annotations by the user.

Ideally, serialize would work similarly and would result in a trait similar to `ArrowDeserialize`:

```rust
trait ArrowSerialize {
    // can be inferred from ArrowField
   // type Source: Iterator<Element=Type>;
   // type Target: Iterator<Element=ArrowType>;
   // can be inferred from MutableArrayAdapter
   type Array: MutableArrayAdapter 
    fn deserialize<S>(source: Source) -> Target;
}

```

However, since both `impl` types cannot be used in traits, we need to explicitly provide a type for the iterator that a MutableArray can 
consume.

- serialize conversion:
    ```rust
    type Source = Collection;
    type Target = Iterator<Element=ArrowType>
    serialize(source: Source) -> Target

trait ArrowSerialize {
    type Source;
    type Target;
    type Array: MutableArrayAdapter 
    fn deserialize<S>(source: Source) -> Target;
}

# Add notes on `Nullable` for `ArrowDeserialize`