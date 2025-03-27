pub fn int_to_bytes(value: &i32) -> [u8; 4] {
    value.to_be_bytes()
}

pub fn string_to_bytes(value: &str) -> &[u8] {
    value.as_bytes()
}
