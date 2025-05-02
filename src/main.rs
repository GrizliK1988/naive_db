mod util;
mod tuple;
mod page;
mod persist;
mod buffer_pool;
mod hash_map;

use buffer_pool::BufferPool;
use page::Page;
use persist::{Reader, Writer};
use tuple::{ Tuple, TupleValue };
use fake::{ faker::name::en::Name, faker::internet::en::FreeEmail, rand::random, Fake };
use std::{ sync::{mpsc::{self, Receiver, Sender}, Arc }, thread::JoinHandle };

fn read_pages_from_disk(threads: u8) -> Vec<(JoinHandle<()>, JoinHandle<()>)> {
    // let mut buffer = BufferPool::new(1000);

    let page_count = Reader::new("./data", "simple.data").page_count();
    println!("Pages {:?}", page_count);

    (0..threads).map(move | i | {
        let (tx, rx) = mpsc::channel();

        let start = (i as u64) * (page_count / threads as u64);
        let end = (i as u64 + 1) * (page_count / threads  as u64);

        println!("{} {}", start, end);

        let handle_read_from_disk = std::thread::spawn(move || {
            let mut reader = Reader::new("./data", "simple.data");
            reader.seek_to_page(start).unwrap();

            for pi in (start..end).step_by(4) {
                let pages = reader.read_page_sequentially(pi).expect("Read failed");

                for p in pages {
                    let p2 = Arc::new(Some(p));
        
                    // println!("Buffer pool write {}", p.id);
                    // buffer.add(p);
                    // let a = buffer.get(pi).unwrap();
            
                    let res = tx.send(p2.clone());
                    match res {
                        Ok(_) => {},
                        Err(e) => {
                            println!("{:?}", e.to_string())
                        }
                    };
                }
            }
        });

        let handle_filter = std::thread::spawn(move || {
            let mut limit = 100;

            'main_loop: for page in rx {
                match &*page {
                    Some(p) => {
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
                    },
                    None => {},
                }
            }
        });

        (handle_read_from_disk, handle_filter)
    }).collect()
}

fn main() {
    let w = Writer::new("./data", "simple.data");

    // 1000 pages
    // for i in 0..1 {
    //     let mut p = Page::new(page_count + i + 1);
    //     // fill page full
    //     for _ in 0..10000 {
    //         let mut name: String = Name().fake();
    //         name.truncate(i16::MAX as usize);
    
    //         let mut email: String = FreeEmail().fake();
    //         email.truncate(i16::MAX as usize);
    
    //         let tuple = Tuple {
    //             types: &[ "integer", "varchar", "varchar" ],
    //             values: vec![ TupleValue::Integer(random::<i32>()), TupleValue::Varchar(name), TupleValue::Varchar(email) ],
    //         };
    
    //         if !p.has_space(&tuple).unwrap() {
    //             break;
    //         }
        
    //         p.write(&tuple).unwrap();
    //     }

    //     w.insert_page(&p).unwrap();
    // }

    // let p = r.read_page(1545).unwrap();
    // let p2 = r.read_page(1547).unwrap();

    // let t = p.read(56, &[ "integer", "varchar", "varchar" ]).unwrap();
    // let t2 = p2.read(11, &[ "integer", "varchar", "varchar" ]).unwrap();

    // println!("{:?}", t);
    // println!("{:?}", t2);

    let handles = read_pages_from_disk(10);

    for (reader, search) in handles {
        reader.join().unwrap();
        search.join().unwrap();
    }
}
