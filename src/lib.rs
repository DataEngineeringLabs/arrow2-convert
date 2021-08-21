// The proc macro is implemented in derive_internal, and re-exported by this
// crate. This is because a single crate can not define both a proc macro and a
// macro_rules macro.
pub use derive_internal::StructOfArrow;

use arrow2::array::StructArray;
use arrow2::datatypes::Field;

pub trait ArrowStruct {
    fn n_fields() -> usize;
    fn field(i: usize) -> Field;
    fn to_arrow(self) -> StructArray;
}
