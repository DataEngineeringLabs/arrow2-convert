use arrow2_convert::ArrowField;
use arrow2_convert::field::LargeBinary;

#[derive(Debug, ArrowField)]
struct Test {
    #[arrow_field(type="LargeBinary")]
    s: String
}

fn main() 
{}
