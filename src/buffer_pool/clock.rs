use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

const BITMAP_CELL_SIZE: usize = 8;
const RETRIES: usize = 10;

#[derive(Debug)]
pub struct Clock {
    size: usize,
    clock: AtomicUsize,
    read_indicator: Vec<AtomicU8>,
}

impl Clock {
    pub fn new(size: usize) -> Self {
        let inidicators_length = (2 * size + BITMAP_CELL_SIZE - 1) / BITMAP_CELL_SIZE;

        Self {
            size,
            clock: AtomicUsize::new(0),
            read_indicator: (0..inidicators_length)
                .into_iter()
                .map(|_| AtomicU8::new(0))
                .collect(),
        }
    }

    pub fn track_read(&self, hash_map_index: &usize) {
        let index = 2 * hash_map_index;

        let bitmap_cell = &self.read_indicator[index / BITMAP_CELL_SIZE];
        let _ = bitmap_cell.fetch_or(1 << (index % BITMAP_CELL_SIZE), Ordering::Release);
    }

    pub fn track_insert(&self, hash_map_index: &usize) {
        let index = 2 * hash_map_index;

        let bitmap_cell = &self.read_indicator[index / BITMAP_CELL_SIZE];
        let _ = bitmap_cell.fetch_or(0b11 << (index % BITMAP_CELL_SIZE), Ordering::Release);
    }

    pub fn track_delete(&self, hash_map_index: &usize) {
        let index = 2 * hash_map_index + 1;

        let bitmap_cell = &self.read_indicator[index / BITMAP_CELL_SIZE];
        let _ = bitmap_cell.fetch_and(!(1 << (index % BITMAP_CELL_SIZE)), Ordering::Release);
    }

    fn mark_unread(&self, hash_map_index: &usize) {
        let index = 2 * hash_map_index;

        let bitmap_cell = &self.read_indicator[index / BITMAP_CELL_SIZE];
        let _ = bitmap_cell.fetch_and(!(1 << (index % BITMAP_CELL_SIZE)), Ordering::Release);
    }

    fn hash_key_status(&self, hash_map_index: &usize) -> (bool, bool) {
        let bitmap_cell = &self.read_indicator[2 * hash_map_index / BITMAP_CELL_SIZE];
        let bitmap_cell_value = bitmap_cell.load(Ordering::Acquire);

        let hash_key_filled_mask = 1 << ((2 * hash_map_index + 1) % BITMAP_CELL_SIZE);
        let hash_key_filled = (bitmap_cell_value & hash_key_filled_mask) > 0;

        let hash_key_accessed_mask = 1 << (2 * hash_map_index % BITMAP_CELL_SIZE);
        let hash_key_accessed = (bitmap_cell_value & hash_key_accessed_mask) > 0;

        (hash_key_filled, hash_key_accessed)
    }

    pub fn find_victim_key(&self) -> Result<usize, ()> {
        for _ in 0..RETRIES * self.size {
            let clock = self.clock.fetch_add(1, Ordering::Relaxed) % self.size;
            let (hash_key_filled, hash_key_accessed) = self.hash_key_status(&clock);

            if !hash_key_filled {
                continue;
            }

            if !hash_key_accessed {
                return Ok(clock);
            } else {
                self.mark_unread(&clock);
            }
        }

        Err(())
    }
}
