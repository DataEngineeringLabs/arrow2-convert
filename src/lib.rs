// The proc macro is implemented in derive_internal, and re-exported by this
// crate. This is because a single crate can not define both a proc macro and a
// macro_rules macro.
pub use derive_internal::ArrowStruct;

pub mod deserialize;
pub mod field;
pub mod serialize;
