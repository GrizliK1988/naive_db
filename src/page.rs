use std::mem;
use crate::tuple::{Tuple, TupleToDataError};

pub const SIZE: usize = 1024 * 8;

// version + number of slots
type Header = (u16, u16);
const HEADER_SIZE: usize = mem::size_of::<Header>();

type SlotId = u16;
type TupleLength = u16;

pub struct Slot {
    pub id: SlotId,
    pub length: TupleLength,
    pub is_thumbstone: u8
}

const SLOT_SIZE: usize = 5;

impl Slot {
    pub fn new(id: SlotId, length: TupleLength) -> Slot {
        Slot {
            id,
            length,
            is_thumbstone: 0,
        }
    }

    pub fn read(slot_data: &[u8]) -> Slot {
        Slot {
            id: SlotId::from_be_bytes([slot_data[0], slot_data[1]]),
            length: TupleLength::from_be_bytes([slot_data[2], slot_data[3]]),
            is_thumbstone: slot_data[4]
        }
    }

    pub fn to_data(&self) -> [u8; SLOT_SIZE] {
        let id_bytes = SlotId::to_be_bytes(self.id);
        let length_bytes = TupleLength::to_be_bytes(self.length);

        [ id_bytes[0], id_bytes[1], length_bytes[0], length_bytes[1], self.is_thumbstone ]
    }

    pub fn length(&self) -> usize {
        self.length as usize
    }
}

pub struct Page {
    pub data: Box<[u8; SIZE]>,
    pub free_space: usize,
    pub slots: usize,
}

impl<'a> Page {
    pub fn new() -> Page {
        let mut data: [u8; SIZE] = [Default::default(); SIZE];

        data[0..2].copy_from_slice(&[0, 1]);
        data[2..4].copy_from_slice(&[0, 0]);

        Page {
            data: Box::new(data),
            free_space: SIZE - HEADER_SIZE,
            slots: 0,
        }
    }

    pub fn from_data(data: [u8; SIZE]) -> Page {
        let slots = u16::from_be_bytes([data[2], data[3]]) as usize;
        let data_size = data[HEADER_SIZE..]
            .chunks(SLOT_SIZE)
            .take(slots)
            .fold(0, | acc, s | acc + Slot::read(s).length());

        Page {
            data: Box::new(data),
            free_space: SIZE - HEADER_SIZE - slots * SLOT_SIZE - data_size,
            slots,
        }
    }

    pub fn has_space(&self, tuple: &Tuple) -> Result<bool, TupleToDataError> {
        let tuple_data = tuple.to_data()?;

        Ok(tuple_data.len() + SLOT_SIZE <= self.free_space)
    }

    pub fn write(&mut self, tuple: &Tuple) -> Result<Slot, TupleToDataError> {
        let tuple_data = tuple.to_data()?;

        if tuple_data.len() > TupleLength::MAX as usize {
            panic!("Can't write a tuple - too big. Overflow pages are not ready");
        }

        let existing_data_length = self.data[HEADER_SIZE..]
            .chunks(SLOT_SIZE)
            .take(self.slots)
            .fold(0, | total_length, slot_data | total_length + Slot::read(slot_data).length());

        let slot_start = self.slots * SLOT_SIZE + HEADER_SIZE;
        let data_start = self.data.len() - existing_data_length;

        let slot = Slot::new(self.slots as SlotId, tuple_data.len() as TupleLength);
        let slot_data = slot.to_data();

        self.slots += 1;
        self.free_space -= slot_data.len() + tuple_data.len();

        self.data[2..4].copy_from_slice(&(self.slots as u16).to_be_bytes());
        self.data[slot_start..slot_start+slot_data.len()].copy_from_slice(&slot_data);
        self.data[data_start-tuple_data.len()..data_start].copy_from_slice(&tuple_data);

        Ok(slot)
    }

    pub fn read(&'a self, slot_id: SlotId, types: &'a[&str]) -> Result<Tuple<'a>, &'a str> {
        let mut data_offset = 0;
        let slot = self.data[HEADER_SIZE..]
            .chunks(SLOT_SIZE)
            .take(self.slots)
            .map(| slot_data | Slot::read(slot_data))
            .find(| slot | {
                data_offset += slot.length();

                slot.id == slot_id
            });

        match slot {
            Some(s) => {
                let data_length = self.data.len();

                Ok(Tuple::read(&types, &self.data[data_length-data_offset..data_length-data_offset+s.length()]))
            },
            None => Err("Cannot read tuple"),
        }
    }

    pub fn read_iterator(&'a self) -> impl Iterator<Item = impl Fn(&'a [&str]) -> Tuple<'a>> {
        let mut data_offset = 0;
        let data_length = self.data.len();

        self.data[HEADER_SIZE..]
            .chunks(SLOT_SIZE)
            .take(self.slots)
            .map(move | slot_data | {
                let slot = Slot::read(slot_data);

                data_offset += slot.length();

                move | types: &'a [&str] | Tuple::read(&types, &self.data[data_length-data_offset..data_length-data_offset+slot.length()])
            })
    }
}
