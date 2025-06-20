use buffer_pool_page_hash_map::BufferPoolPageHashMap;
use crate::{buffer_pool_page_hash_map::InsertPageResult::{NewPage, ExistingPage}, tuple::{Tuple, TupleValue}};

mod util {
    pub mod type_converter {
        include!("../src/util/type_converter.rs");
    }

    pub mod free_list {
        include!("../src/util/free_list.rs");
    }
}

mod tuple {
    include!("../src/tuple.rs");
}

mod page {
    include!("../src/page.rs");
}

mod buffer_pool_page_hash_map {
    include!("../src/buffer_pool_page_hash_map.rs");
}

#[test]
fn test_simple() {
    let m = BufferPoolPageHashMap::new(100);

    {
        let Ok(NewPage(mut page)) = m.insert_page(&1) else {
            panic!("Cannot insert page");
        };
        let _ = page.write(&Tuple { types: &["integer"], values: vec![TupleValue::Integer(15)] });
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
                        },
                        Ok(ExistingPage(_)) => {
                            println!("Existing page {}", id);
                        },
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

// #[test]
// fn test_conflicting_inserts_with_deletes() {
//     let mut m = LinearIndirectPageHashMap::new(5);
//     let mut ids: VecDeque<u64> = vec![31, 44, 53, 78, 87, 104, 106, 125, 126, 127, 128].into();

//     {
//         let mut inserted_ids = VecDeque::new();

//         for _ in 0..5 {
//             let id = ids.pop_front().unwrap();
//             let p = Page::new(id);
//             m.insert(p).unwrap();
//             inserted_ids.push_back(id);
//         }

//         let p = Page::new(ids.pop_front().unwrap());
//         let result = m.insert(p);
//         assert_eq!(true, result.is_err());

//         for _ in 0..5 {
//             let id = inserted_ids.pop_front().unwrap();
//             let p = m.get(id).unwrap();
//             assert_eq!(id, p.0.as_ref().unwrap().id);
//         }

//         m.delete(&53).unwrap();

//         let p = Page::new(12);
//         m.insert(p).unwrap();
//     }

//     assert_eq!(m.get(31).unwrap().0.as_ref().unwrap().id, 31);
//     assert_eq!(m.get(44).unwrap().0.as_ref().unwrap().id, 44);
//     assert_eq!(m.get(12).unwrap().0.as_ref().unwrap().id, 12);
//     assert_eq!(m.get(78).unwrap().0.as_ref().unwrap().id, 78);
//     assert_eq!(m.get(87).unwrap().0.as_ref().unwrap().id, 87);

//     assert_eq!(m.get(5).is_none(), true);
// }

// #[test]
// fn test_delete() {
//     let mut m = LinearIndirectPageHashMap::new(5);

//     {
//         m.insert(Page::new(31)).unwrap();
//         assert_eq!(m.get(31).unwrap().0.as_ref().unwrap().id, 31);
//     }

//     {
//         m.delete(&31).unwrap();
//         assert_eq!(m.get(31).is_none(), true);
//     }

//     {
//         m.insert(Page::new(44)).unwrap();
//         assert_eq!(m.get(44).unwrap().0.as_ref().unwrap().id, 44);
//     }
// }

// #[test]
// fn test_insert_multithread_large() {
//     for _ in 0..10 {
//         let m = LinearIndirectPageHashMap::new(5000);
    
//         std::thread::scope(|s| {
//             for id in 0..5000 {
//                 let m = Arc::new(&m);
//                 s.spawn(move || {
//                     let _ = &m.insert(Page::new(id));
//                 });
//             }
//         });
    
//         for id in 0..5000 {
//             assert_eq!(id, m.get(id).unwrap().0.as_ref().unwrap().id);
//         }
//     }
// }

// #[test]
// fn test_insert_multithread_simple_with_deletes() {
//     for _ in 0..1000 {
//         let mut ids: VecDeque<u64> = vec![31, 44, 53, 78, 87, 104, 106, 125, 126, 127, 128].into();
//         let m = LinearIndirectPageHashMap::new(5);
    
//         std::thread::scope(|s| {
//             let (tx, rx) = std::sync::mpsc::channel::<u64>();

//             let mm = Arc::new(&m);
//             s.spawn(move || {
//                 for i in rx {
//                     mm.delete(&i).unwrap();
//                 }
//             });

//             for _ in 0..5 {
//                 let id = ids.pop_front().unwrap();
//                 let m = Arc::new(&m);
//                 let tx = tx.clone();
//                 s.spawn(move || {
//                     let _ = &m.insert(Page::new(id));

//                     if id == 44 || id == 78 {
//                         tx.send(id).unwrap();
//                     } 
//                 });
//             }
//         });

//         m.insert(Page::new(14)).unwrap();
//         m.insert(Page::new(77)).unwrap();
    
//         for id in [31, /*44, */53, /*78, */87, 14, 77] {
//             assert_eq!(id, m.get(id).unwrap().0.as_ref().unwrap().id);
//         }
//     }
// }
