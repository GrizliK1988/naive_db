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

use util::free_list::{ConcurrentFreeList, FreeList};
use std::sync::{atomic::Ordering, Arc};

use crate::{page::Page, util::free_list::AllocatedPage};

#[test]
fn test_free_list() {
    {
        let mut free_list = FreeList {
            value: Some(10),
            next: None
        };

        let v = free_list.release();
        assert_eq!(v, Some(10));
        assert_eq!(free_list.value, None);
        assert_eq!(free_list.next, None);
    }

    {
        let mut free_list = FreeList::new(vec![0, 1, 2]);

        println!("{:?}", free_list);

        {
            let v = free_list.release();
            assert_eq!(v, Some(0));
            assert_ne!(free_list.value, None);
            assert_ne!(free_list.next, None);
        }

        {
            free_list.add(0);
            assert_eq!(free_list.value, Some(0));
            assert_ne!(free_list.next, None);
        }

        {
            let v = free_list.release();
            assert_eq!(v, Some(0));
            assert_ne!(free_list.value, None);
            assert_ne!(free_list.next, None);
        }

        {
            let v = free_list.release();
            assert_eq!(v, Some(1));
            assert_ne!(free_list.value, None);
            assert_eq!(free_list.next, None);
        }

        {
            let v = free_list.release();
            assert_eq!(v, Some(2));
            assert_eq!(free_list.value, None);
            assert_eq!(free_list.next, None);
        }

        {
            let v = free_list.release();
            assert_eq!(v, None);
            assert_eq!(free_list.value, None);
            assert_eq!(free_list.next, None);
        }
    }
}

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
fn test_concurrent_free_list_allocate() {
    {
        let size = 100000;
        let free_list = Arc::new(ConcurrentFreeList::new((0..size).collect()));

        let vec = (0..size).collect::<Vec<usize>>();
        let chunks = vec.chunks(100);

        std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel();

            s.spawn(move || {
                let mut results = rx.iter().collect::<Vec<Result<usize, ()>>>();

                assert_eq!(results.len(), size);

                let r: Vec<&[Result<usize, ()>]> = results.chunk_by(|x, y| x.unwrap() == y.unwrap()).collect();
                assert_eq!(r.len(), size);

                results
                    .sort_by(| x, y | {
                        let x = x.unwrap();
                        let y = y.unwrap();

                        x.cmp(&y)
                    });

                let elements = results.iter().map(| x | x.unwrap()).collect::<Vec<usize>>();
                assert_eq!(elements, (0..size).collect::<Vec<usize>>());
            });

            for chunk in chunks {
                let tx = tx.clone();
                let free_list = Arc::clone(&free_list);

                s.spawn(move || {
                    for _ in chunk {
                        let result = free_list.allocate();
                        tx.send(result).unwrap();
                    }
                });
            }
        });
    }
}

#[test]
fn test_concurrent_free_list_allocate_with_overallocation() {
    {
        let size = 10000;
        let overflow = 100;
        let free_list = Arc::new(ConcurrentFreeList::new((0..size).collect()));

        let vec = (0..size+overflow).collect::<Vec<usize>>();
        let chunks = vec.chunks(50);

        std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel();

            s.spawn(move || {
                let results = rx.iter().collect::<Vec<Result<usize, ()>>>();

                let elements = results.iter().filter(|&x| x.is_ok()).map(|x| x.unwrap()).collect::<Vec<usize>>();
                let failures = results.iter().filter(|&x| !x.is_ok()).collect::<Vec<&Result<usize, ()>>>();

                let r: Vec<&[usize]> = elements.chunk_by(|x, y| x == y).collect();
                assert_eq!(r.len(), size);

                assert_eq!(elements.len(), size);
                assert_eq!(failures.len(), overflow);
            });

            for chunk in chunks {
                let tx = tx.clone();
                let free_list = Arc::clone(&free_list);

                s.spawn(move || {
                    for _ in chunk {
                        let result = free_list.allocate();
                        tx.send(result).unwrap();
                    }
                });
            }
        });
    }
}

#[test]
fn test_concurrent_free_list_deallocate() {
    {
        let size = 10000;
        let free_list = Arc::new(ConcurrentFreeList::new(vec![]));

        let vec = (0..size).collect::<Vec<usize>>();
        let chunks = vec.chunks(50);

        std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel();

            let free_list_clone = Arc::clone(&free_list);
            s.spawn(move || {
                let results = rx.iter().collect::<Vec<Result<(), ()>>>();

                assert_eq!(results.iter().filter(|x| x.is_ok()).collect::<Vec<&Result<(), ()>>>().len(), size);

                let mut r = vec![];
                loop {
                    let el = free_list_clone.allocate();
                    match el {
                        Ok(el) => r.push(el),
                        Err(_) => break,
                    }
                }

                assert_eq!(r.len(), size);
            });

            for chunk in chunks {
                let tx = tx.clone();
                let free_list = Arc::clone(&free_list);

                s.spawn(move || {
                    for element in chunk {
                        let result = free_list.deallocate(element);
                        tx.send(result).unwrap();
                    }
                });
            }
        });
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

                let r: Vec<&[Result<AllocatedPage, ()>]> = results.chunk_by(|x, y| x.as_ref().unwrap().free_list_id == y.as_ref().unwrap().free_list_id).collect();
                assert_eq!(r.len(), size);

                results
                    .sort_by(| x, y | {
                        let x = x.as_ref().unwrap().free_list_id;
                        let y = y.as_ref().unwrap().free_list_id;

                        x.cmp(&y)
                    });

                let elements = results.iter().map(| x | x.as_ref().unwrap().free_list_id).collect::<Vec<usize>>();
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
                let results = rx.iter().collect::<Vec<Result<(), _>>>();

                assert_eq!(results.iter().filter(|x| x.is_ok()).collect::<Vec<&Result<(), _>>>().len(), size);

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
                        let result = free_list.deallocate_page(element);
                        tx.send(result).unwrap();
                    }
                });
            }
        });
    }
}
