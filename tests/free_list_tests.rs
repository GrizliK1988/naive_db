pub mod tuple {
    include!("../src/tuple.rs");
}

pub mod page {
    include!("../src/page.rs");
}

mod util {
    pub mod type_converter {
        include!("../src/util/type_converter.rs");
    }

    pub mod free_list {
        include!("../src/util/free_list.rs");
    }
}

use std::sync::atomic::Ordering;
use util::free_list::ConcurrentFreeList;

use crate::util::free_list::AllocatedPage;

#[test]
fn test_concurrent_free_list() {
    {
        let free_list = ConcurrentFreeList::new(vec![0, 1, 2]);

        println!("{:?}", free_list.next.load(Ordering::Relaxed).is_null());

        let next = unsafe { Box::from_raw(free_list.next.load(Ordering::Relaxed)) };
        println!("{:?}", (*next).value);
        println!("{:?}", (*next).next.load(Ordering::Relaxed).is_null());

        let next = unsafe { Box::from_raw((*next).next.load(Ordering::Relaxed)) };
        println!("{:?}", (*next).value);
        println!("{:?}", (*next).next.load(Ordering::Relaxed).is_null());

        let next = unsafe { Box::from_raw((&*next).next.load(Ordering::Relaxed)) };
        println!("{:?}", (*next).value);
        println!("{:?}", (*next).next.load(Ordering::Relaxed).is_null());
    }
}

#[test]
fn test_concurrent_free_list_allocate_page() {
    {
        let size = 100000;
        let free_list = ConcurrentFreeList::new((0..size).collect());

        let vec = (0..size).collect::<Vec<usize>>();
        let chunks = vec.chunks(100);

        std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel();

            s.spawn(move || {
                let mut results = rx.iter().collect::<Vec<Result<AllocatedPage, ()>>>();

                assert_eq!(results.len(), size);

                let r: Vec<&[Result<AllocatedPage, ()>]> = results
                    .chunk_by(|x, y| {
                        x.as_ref().unwrap().free_list_id == y.as_ref().unwrap().free_list_id
                    })
                    .collect();
                assert_eq!(r.len(), size);

                results.sort_by(|x, y| {
                    let x = x.as_ref().unwrap().free_list_id;
                    let y = y.as_ref().unwrap().free_list_id;

                    x.cmp(&y)
                });

                let elements = results
                    .iter()
                    .map(|x| x.as_ref().unwrap().free_list_id)
                    .collect::<Vec<usize>>();
                assert_eq!(elements, (0..size).collect::<Vec<usize>>());
            });

            for chunk in chunks {
                let tx = tx.clone();
                let free_list = &free_list;

                s.spawn(move || {
                    for _ in chunk {
                        let result = free_list.allocate_page();
                        tx.send(result).unwrap();
                    }
                });
            }
        });
    }
}

#[test]
fn test_concurrent_free_list_deallocate_page() {
    {
        let size = 10000;
        let free_list = ConcurrentFreeList::new((0..size).collect());

        let mut chunks = vec![];

        for chunk in (0..size).collect::<Vec<usize>>().chunks(50) {
            let mut v = vec![];
            for _ in chunk {
                v.push(free_list.allocate_page().unwrap());
            }
            chunks.push(v);
        }

        std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel();

            let free_list_clone = &free_list;
            s.spawn(move || {
                for _ in rx {}

                let mut r = vec![];
                loop {
                    let el = free_list_clone.allocate_page();
                    match el {
                        Ok(el) => r.push(el.free_list_id),
                        Err(_) => break,
                    }
                }

                assert_eq!(r.len(), size);
            });

            for chunk in chunks {
                let tx = tx.clone();
                let free_list = &free_list;

                s.spawn(move || {
                    for element in chunk {
                        free_list.deallocate_page(element);
                        tx.send(true).unwrap();
                    }
                });
            }
        });
    }
}
