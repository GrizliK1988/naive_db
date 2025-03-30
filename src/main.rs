mod util;
mod tuple;
mod page;
mod persist;

use page::Page;
use tuple::{ Tuple, TupleValue };

fn main() {
    let mut p = Page::new();

    let t1 = Tuple {
        types: &["integer", "integer"],
        values: vec![TupleValue::Integer(10), TupleValue::Integer(22)],
    };
    
    p.write(&t1);
}
