mod util {
    pub mod type_converter {
        include!("../src/util/type_converter.rs");
    }
}

use util::type_converter::{ int_to_bytes, bytes_to_int, string_to_bytes, bytes_to_string };

#[test]
fn test_int_to_bytes() {
    let result_1 = int_to_bytes(&0);
    assert_eq!(result_1, [0, 0, 0, 0]);

    let result_1_rev = bytes_to_int(&result_1);
    assert_eq!(result_1_rev, 0);

    let result_2 = int_to_bytes(&1);
    assert_eq!(result_2, [0, 0, 0, 1]);

    let result_2_rev = bytes_to_int(&result_2);
    assert_eq!(result_2_rev, 1);

    let result_3 = int_to_bytes(&-256);
    assert_eq!(result_3, [255, 255, 255, 0]);

    let result_3_rev = bytes_to_int(&result_3);
    assert_eq!(result_3_rev, -256);
}

#[test]
fn test_string_to_bytes() {
    let input = String::from("Hello!");
    let result_1 = string_to_bytes(&input);
    assert_eq!(result_1, [72, 101, 108, 108, 111, 33]);

    let result_1_rev = bytes_to_string(result_1);
    assert_eq!(result_1_rev, "Hello!");


    let mut arr = [0; 5];
    arr[1..1+2].copy_from_slice(&[1, 2]);

    assert_eq!(arr, [0, 1, 2, 0, 0]);
}