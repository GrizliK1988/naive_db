use crate::{
    buffer_pool::page_hash_map::{
        BufferPoolPageHashMap,
        InsertPageResult::{ExistingPage, NewPage},
    },
    tuple::{Tuple, TupleValue},
};

mod util {
    include!("../src/util/mod.rs");
}

mod tuple {
    include!("../src/tuple.rs");
}

mod page {
    include!("../src/page.rs");
}

mod persist {
    include!("../src/persist.rs");
}

mod buffer_pool {
    include!("../src/buffer_pool/mod.rs");
}

#[test]
fn test_simple() {
    let m = BufferPoolPageHashMap::new(100);

    {
        let Ok(NewPage(mut page)) = m.insert_page(&1) else {
            panic!("Cannot insert page");
        };
        let _ = page.write(&Tuple {
            types: &["integer"],
            values: vec![TupleValue::Integer(15)],
        });
        page.id = 1;
    }

    {
        let page = m.read_page(&1).unwrap();
        let tuple = page.read(0, &["integer"]).unwrap();

        assert_eq!(tuple.values[0], TupleValue::Integer(15));
    }
}

#[test]
fn test_insert_multithread_simple() {
    let m = &BufferPoolPageHashMap::new(500);

    std::thread::scope(|s| {
        for _ in 0..10 {
            s.spawn(move || {
                for id in 0..50 {
                    match m.insert_page(&id) {
                        Ok(NewPage(mut guard)) => {
                            println!("New page {}", id);

                            guard.id = id;
                            guard.data[67] = 1;
                            guard.data[69] = 7;
                        }
                        Ok(ExistingPage(_)) => {
                            println!("Existing page {}", id);
                        }
                        Err(err) => {
                            println!("Error for page {} {:?}", id, err);
                        }
                    };
                }
            });
        }
    });

    for id in 0..50 {
        assert_eq!(id, m.read_page(&id).unwrap().id);
    }
}
