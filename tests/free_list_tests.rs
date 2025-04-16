mod util {
    pub mod free_list {
        include!("../src/util/free_list.rs");
    }
}

use util::free_list::FreeList;

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
