use arrow2::array::*;
use arrow2::datatypes::{DataType, Field};
use arrow2_derive::StructOfArrow;

#[derive(Debug, Clone, PartialEq, StructOfArrow)]
#[arrow2_derive = "Default, Debug"]
pub struct Particle {
    pub name: String,
    pub mass: f64,
}

trait ArrowStruct {
    fn n_fields() -> usize;
    fn field(i: usize) -> Field;
    fn to_arrow(self) -> StructArray;
}

impl ParticleArray {
    fn push(&mut self, name: Option<&str>, mass: Option<f64>) {
        self.name.push(name);
        self.mass.push(mass);
    }
}

impl ArrowStruct for ParticleArray {
    fn n_fields() -> usize {
        // to macro
        1
    }

    fn field(i: usize) -> Field {
        // to macro
        match i {
            0 => Field::new("name", DataType::Utf8, true),
            _ => panic!(),
        }
    }

    fn to_arrow(self) -> StructArray {
        let fields = (0..ParticleArray::n_fields())
            .map(ParticleArray::field)
            .collect();
        // to macro
        let Self { name, mass } = self;
        let values = vec![name.into_arc(), mass.into_arc()];

        StructArray::from_data(fields, values, None)
    }
}

#[test]
fn new() {
    let mut a = ParticleArray::new();
    a.push(Some("a"), Some(0.1));
    let debug = format!("{:#?}", a);
    println!("{:#?}", a);
    assert_eq!(
        debug,
        r#"ParticleArray {
    name: MutableUtf8Array {
        offsets: [
            0,
            1,
        ],
        values: [
            97,
        ],
        validity: None,
    },
    mass: MutablePrimitiveArray {
        data_type: Float64,
        values: [
            0.1,
        ],
        validity: None,
    },
}"#
    );
}
