mod buffer_pool;
mod hash_map;
mod page;
mod persist;
mod tuple;
mod util;

use persist::Reader;

use crate::{buffer_pool::buffer_pool::BufferPool, tuple::TupleValue};

fn main() {
    let reader = Reader::new("./data", "simple.data");
    let page_number = reader.page_count();
    let pool = BufferPool::new(2 ^ 17, reader);

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

                for i in 0..page_number {
                    let page = pool.get(i).unwrap();

                    for tuple_data in page.get().read_iterator_raw() {
                        // let id = i32::from_be_bytes(tuple_data[0..4].try_into().unwrap());
                        let id = ((tuple_data[0] as i32) << 24)
                            | ((tuple_data[1] as i32) << 16)
                            | ((tuple_data[2] as i32) << 8)
                            | (tuple_data[3] as i32);

                        // let TupleValue::Integer(id) = tuple.values[0] else {
                        //     panic!("First value of a tuple is not integer");
                        // };

                        if id < 140651032 && id > 140641012 {
                            // println!("Found in page {}. id: {}", i, id);
                        }
                    }
                }

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
