#![forbid(unsafe_code)]

// The proc macro is implemented in derive_internal, and re-exported by this
// crate. This is because a single crate can not define both a proc macro and a
// macro_rules macro.
pub mod deserialize;
pub mod field;
pub mod serialize;

#[cfg(feature = "arrow2_convert_derive")]
#[doc(hidden)]
pub use arrow2_convert_derive::ArrowField;

// Test README with doctests
#[doc = include_str!("../README.md")]
#[cfg(doctest)]
struct ReadmeDoctests;
