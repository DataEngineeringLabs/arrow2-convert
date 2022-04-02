use arrow2::array::Array;
use arrow2_convert::field::FixedSizeBinary;
use arrow2_convert::serialize::*;

#[test]
fn test_error_exceed_fixed_size_binary() {
    let strs = [b"abc".to_vec()];
    let r: arrow2::error::Result<Box<dyn Array>> =
        strs.try_into_arrow_as_type::<FixedSizeBinary<2>>();
    assert!(r.is_err())
}
