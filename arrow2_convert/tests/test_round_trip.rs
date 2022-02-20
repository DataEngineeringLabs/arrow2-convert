use arrow2::array::*;
use arrow2_convert::deserialize::*;
use arrow2_convert::serialize::*;
use arrow2_convert_derive::ArrowField;

#[test]
fn test_nested_optional_struct_array() {
    #[derive(Debug, Clone, ArrowField, PartialEq)]
    struct Top {
        child_array: Vec<Option<Child>>,
    }
    #[derive(Debug, Clone, ArrowField, PartialEq)]
    struct Child {
        a1: i64,
    }

    let original_array = vec![
        Top {
            child_array: vec![
                Some(Child { a1: 10 }),
                None,
                Some(Child { a1: 12 }),
                Some(Child { a1: 14 }),
            ],
        },
        Top {
            child_array: vec![None, None, None, None],
        },
        Top {
            child_array: vec![None, None, Some(Child { a1: 12 }), None],
        },
    ];

    let b: Box<dyn Array> = original_array.into_arrow().unwrap();
    let round_trip: Vec<Top> = b.try_into_iter().unwrap();
    assert_eq!(original_array, round_trip);
}
