use crate::buffer_pool::clock::Clock;

mod util {
    include!("../src/util/mod.rs");
}

mod tuple {
    include!("../src/tuple.rs");
}

mod page {
    include!("../src/page.rs");
}

mod buffer_pool {
    include!("../src/buffer_pool/mod.rs");
}

#[test]
fn test_simple() {
    let c = Clock::new(8);

    {
        c.track_insert(&0);
        c.track_insert(&1);
        c.track_insert(&2);

        let r = c.find_victim_key();
        assert_eq!(0, r.unwrap());

        c.track_insert(&1);

        let r = c.find_victim_key();
        assert_eq!(2, r.unwrap());
    }
}

#[test]
fn test_concurrent_find_victim_key() {
    let size = 1024;

    let c = Clock::new(size);
    let c_ref = &c;

    for i in 0..size {
        c.track_insert(&i);
    }

    for _ in 0..10 {
        std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel();

            s.spawn(move || {
                let mut keys: Vec<usize> = rx.iter().collect();
                keys.sort();

                let expected_keys: Vec<usize> = (0..size).collect();

                assert_eq!(keys, expected_keys);
            });

            for _ in 0..size {
                let tx = tx.clone();
                s.spawn(move || {
                    let key = c_ref.find_victim_key().unwrap();
                    tx.send(key).unwrap();
                });
            }
        });
    }
}

#[test]
fn test_concurrent_track_delete() {
    let size = 1024;

    let c = Clock::new(size);
    let c_ref = &c;

    for i in 0..size {
        c.track_insert(&i);
    }

    std::thread::scope(|s| {
        for i in 0..size {
            s.spawn(move || {
                c_ref.track_delete(&i);
            });
        }
    });

    assert_eq!(true, c.find_victim_key().is_err());
}
