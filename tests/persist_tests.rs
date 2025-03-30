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

mod persist {
    include!("../src/persist.rs");
}

use std::fs;

use page::Page;
use persist::{Reader, Writer};
use tuple::{Tuple, TupleValue};
use fake::{ faker::name::en::Name, faker::internet::en::FreeEmail, rand::random, Fake };

#[test]
fn test_persist_single_page() {
    let mut p = Page::new();

    for _ in 0..10000 {
        let mut name: String = Name().fake();
        name.truncate(i8::MAX as usize - 1);

        let mut email: String = FreeEmail().fake();
        email.truncate(i8::MAX as usize - 1);

        let tuple = Tuple {
            types: &[ "integer", "varchar", "varchar" ],
            values: vec![ TupleValue::Integer(random::<i32>()), TupleValue::Varchar(name), TupleValue::Varchar(email) ],
        };

        if !p.has_space(&tuple).unwrap() {
            break;
        }
    
        p.write(&tuple).unwrap();
    }

    println!("Page prepared. Slots: {}. Spare space: {}", p.slots, p.free_space);

    fs::remove_file("./01_single_page").unwrap();

    let writer = Writer::new(".", "01_single_page");
    writer.insert_page(&p).unwrap();

    let reader = Reader::new(".", "01_single_page");
    let read_page = reader.read_page(0).unwrap();

    assert_eq!(p.data, read_page.data);

    fs::remove_file("./01_single_page").unwrap();
}

