pub fn int_to_bytes(value: &i32) -> [u8; 4] {
    value.to_be_bytes()
}

pub fn bytes_to_int(bytes: &[u8; 4]) -> i32 {
    i32::from_be_bytes(*bytes)
}

pub fn string_to_bytes(value: &str) -> &[u8] {
    value.as_bytes()
}

pub fn bytes_to_string(bytes: &[u8]) -> String {
    unsafe {
        String::from_utf8_unchecked(bytes.to_vec())
    }
} 
