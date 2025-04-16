use std::{mem, num::TryFromIntError};

use crate::util::type_converter::{int_to_bytes, string_to_bytes};

#[derive(Debug)]
pub enum TupleValue {
    Integer(i32),
    Varchar(String),
}

#[derive(Debug)]
pub struct Tuple<'a> {
    pub types: &'a[&'a str],
    pub values: Vec<TupleValue>,
}

pub type VarcharLength = u16;

#[derive(Debug)]
pub enum TypeConversionError {
    IntConversionError(TryFromIntError),
}

#[derive(Debug)]
pub enum TupleToDataError {
    TypeConversionError(TypeConversionError),
}

impl <'a> From<TryFromIntError> for TupleToDataError {
    fn from(err: TryFromIntError) -> TupleToDataError {
        TupleToDataError::TypeConversionError(TypeConversionError::IntConversionError(err))
    }
}

impl<'a> Tuple<'a> {
    pub fn read(types: &'a[&str], data: &[u8]) -> Tuple<'a> {
        let mut current_offset = 0;
        let mut values = Vec::with_capacity(types.len());

        for &t in types {
            let value = match t {
                s if s.eq_ignore_ascii_case("integer") => {
                    let bytes: [u8; 4] = data[current_offset..current_offset+mem::size_of::<i32>()]
                        .try_into()
                        .unwrap_or_else(| _ | panic!("Can't parse value to i32"));

                    current_offset += bytes.len();

                    TupleValue::Integer(i32::from_be_bytes(bytes))
                },
                s if s.eq_ignore_ascii_case("varchar") => {
                    let string_length = VarcharLength::from_be_bytes([ data[current_offset], data[current_offset+1] ]);
                    let bytes = data[current_offset+2..current_offset+2+(string_length as usize)].to_vec();

                    current_offset += bytes.len() + std::mem::size_of::<VarcharLength>();

                    unsafe {
                        TupleValue::Varchar(String::from_utf8_unchecked(bytes))
                    }
                },
                _ => panic!("Unsupported type {}", t),
            };

            values.push(value);
        }

        Tuple {
            types,
            values,
        }
    }

    pub fn to_data(&self) -> Result<Vec<u8>, TupleToDataError> {
        let mut new_tuple: Vec<u8> = Vec::new();
        for v in self.values.iter() {
            let bytes: &[u8] = match v {
                TupleValue::Integer(i) => &int_to_bytes(i),
                TupleValue::Varchar(i) => {
                    let string_bytes = string_to_bytes(i);
                    let len: VarcharLength = string_bytes.len().try_into()?;

                    &[len.to_be_bytes().as_slice(), string_bytes].concat()
                },
            };

            new_tuple.extend_from_slice(bytes);
        }

        Ok(new_tuple)
    }
}

impl<'a> PartialEq for Tuple<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.types == other.types && self.values == other.values
    }
}

impl<'a> PartialEq for TupleValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Integer(l0), Self::Integer(r0)) => l0 == r0,
            (Self::Varchar(l0), Self::Varchar(r0)) => l0 == r0,
            _ => false,
        }
    }
}