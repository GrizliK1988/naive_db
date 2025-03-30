mod util {
    pub mod type_converter {
        include!("../src/util/type_converter.rs");
    }
}

mod page {
    include!("../src/page.rs");
}

mod tuple {
    include!("../src/tuple.rs");
}

use page::Page;
use tuple::{Tuple, TupleValue};

#[test]
fn test_create_page() {
    let mut p = Page::new();

    assert_eq!(p.slots, 0);
    assert_eq!(p.free_space, 1024 * 8 - 4);

    {
        /* Tuple 0 */
        let tuple = Tuple {
            types: &[ "integer", "varchar" ],
            values: vec![ TupleValue::Integer(10), TupleValue::Varchar("Hello!".to_owned()) ],
        };
    
        let slot = p.write(&tuple).unwrap();
        assert_eq!(slot.id, 0);
    
        let tuple_read = p.read(slot.id, &["integer", "varchar"]).unwrap();
        assert_eq!(tuple_read, Tuple {
            types: &[ "integer", "varchar" ],
            values: vec![TupleValue::Integer(10), TupleValue::Varchar("Hello!".to_owned())],
        });
    }

    {
        let tuple = Tuple {
            types: &[ "varchar", "varchar" ],
            values: vec![ TupleValue::Varchar("It's me again".to_owned()), TupleValue::Varchar("lalalala".to_owned()) ],
        };

        let slot = p.write(&tuple).unwrap();
        assert_eq!(slot.id, 1);

        let tuple_read = p.read(slot.id, &["varchar", "varchar"]).unwrap();
        assert_eq!(tuple_read, Tuple {
            types: &[ "varchar", "varchar" ],
            values: vec![TupleValue::Varchar("It's me again".to_owned()), TupleValue::Varchar("lalalala".to_owned())],
        });
    }

    {
        let tuple = Tuple {
            types: &[ "varchar", "varchar", "integer" ],
            values: vec![
                TupleValue::Varchar("It's me again heeey".to_owned()),
                TupleValue::Varchar("test test".to_owned()),
                TupleValue::Integer(25),
            ],
        };

        let slot = p.write(&tuple).unwrap();
        assert_eq!(slot.id, 2);

        assert_eq!(p.has_space(&tuple).unwrap(), true);

        let tuple_read = p.read(slot.id, &["varchar", "varchar", "integer"]).unwrap();
        assert_eq!(tuple_read, Tuple {
            types: &[ "varchar", "varchar", "integer" ],
            values: vec![TupleValue::Varchar("It's me again heeey".to_owned()), TupleValue::Varchar("test test".to_owned()), TupleValue::Integer(25)],
        });
    }

    {
        let vec = vec![b' '; 20_000];
        let s = String::from_utf8(vec).expect("Invalid UTF-8 string");

        let tuple = Tuple {
            types: &[ "varchar" ],
            values: vec![
                TupleValue::Varchar(s),
            ],
        };

        assert_eq!(p.has_space(&tuple).unwrap(), false);
    }

    {
        let vec = vec![b' '; 7_000];
        let s = String::from_utf8(vec).expect("Invalid UTF-8 string");

        let tuple = Tuple {
            types: &[ "varchar" ],
            values: vec![
                TupleValue::Varchar(s),
            ],
        };

        assert_eq!(p.has_space(&tuple).unwrap(), true);
    }
}
