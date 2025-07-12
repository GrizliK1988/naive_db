#![cfg_attr(test, allow(dead_code))]
mod buffer_pool;
mod page;
mod persist;
mod tuple;
mod util;

use persist::Reader;

use crate::buffer_pool::buffer_pool::BufferPool;

fn main() {
    let reader = Reader::new("./data", "simple.data");
    let page_number = reader.page_count();
    let pool = BufferPool::new(2 ^ 17, reader);
    let pool_ref = &pool;

    loop {
        println!("Select an action:");
        println!("1 - continue");
        println!("2 - show number of pages");

        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");

        match input.trim() {
            "1" => {
                println!("Searching...");

                let start_time = std::time::Instant::now();

                std::thread::scope(|s| {
                    for j in 0..8 {
                        s.spawn(move || {
                            for i in j * page_number / 8..(j + 1) * page_number / 8 {
                                if i >= page_number {
                                    break;
                                }

                                let result = pool_ref.get(i);
                                let Ok(page) = result else {
                                    println!("Page cant be read {:?}", result.err().unwrap());
                                    panic!("");
                                };

                                for tuple_data in page.get().read_iterator_raw() {
                                    let id =
                                        i32::from_be_bytes(tuple_data[0..4].try_into().unwrap());

                                    if id < 140651032 && id > 140641012 {
                                        println!("Found in page {}. id: {}", i, id);
                                    }
                                }
                            }
                        });
                    }
                });

                let duration = start_time.elapsed();
                println!("Time taken: {:?}", duration);
            }
            "2" => {
                println!("There are {} pages", page_number);
            }
            _ => {
                println!("Invalid input, please try again.");
            }
        }
    }
}
