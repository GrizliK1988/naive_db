mod util;
mod tuple;
mod page;
mod persist;

use page::Page;
use persist::{Reader, Writer};
use tuple::{ Tuple, TupleValue };
use fake::{ faker::name::en::Name, faker::internet::en::FreeEmail, rand::random, Fake };

fn main() {
    let w = Writer::new("./data", "simple.data");

    // 1000 pages
    for _ in 0..1 {
        let mut p = Page::new();
        // fill page full
        for _ in 0..10000 {
            let mut name: String = Name().fake();
            name.truncate(i16::MAX as usize);
    
            let mut email: String = FreeEmail().fake();
            email.truncate(i16::MAX as usize);
    
            let tuple = Tuple {
                types: &[ "integer", "varchar", "varchar" ],
                values: vec![ TupleValue::Integer(random::<i32>()), TupleValue::Varchar(name), TupleValue::Varchar(email) ],
            };
    
            if !p.has_space(&tuple).unwrap() {
                break;
            }
        
            p.write(&tuple).unwrap();
        }

        w.insert_page(&p).unwrap();
    }

    let mut r = Reader::new("./data", "simple.data");
    let p = r.read_page(1545).unwrap();
    let p2 = r.read_page(1547).unwrap();

    let t = p.read(56, &[ "integer", "varchar", "varchar" ]).unwrap();
    let t2 = p2.read(11, &[ "integer", "varchar", "varchar" ]).unwrap();

    println!("{:?}", t);
    println!("{:?}", t2);

    println!("Pages {:?}", r.page_count());

    let mut limit = 100;

    'main_loop: for pi in 0..r.page_count() {
        let p = r.read_page(pi).unwrap();

        for s in p.read_iterator() {
            let t = s(&[ "integer", "varchar", "varchar" ]);
            let r = match t.values[0] {
                TupleValue::Integer(i) => {
                    i > 1000 && i < 1500
                },
                _ => false
            };

            if r {
                println!("Found Tuple {:?}", t);
                limit -= 1;

                if limit < 1 {
                    break 'main_loop;
                }
            }
        }
    }
}
