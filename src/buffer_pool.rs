use std::sync::{atomic::{AtomicU8, AtomicUsize, Ordering}, RwLockReadGuard};

use crate::{hash_map::LinearIndirectPageHashMap, page::{Page, PageId}};

const BITMAP_CELL_SIZE: usize = 8;

pub struct BufferPool<'a> {
    pub page_map: LinearIndirectPageHashMap<'a>,
    size: usize,
    clock: AtomicUsize,
    read_indicator: Vec<AtomicU8>,
}

impl<'a> BufferPool<'a> {
  pub fn new(size: usize) -> BufferPool<'a> {
    BufferPool {
      size,
      page_map: LinearIndirectPageHashMap::new(size),
      clock: AtomicUsize::new(0),
      read_indicator: (0..size/BITMAP_CELL_SIZE+1).into_iter().map(|_| AtomicU8::new(0)).collect()
    } 
  }

  pub fn get(&self, page_id: PageId) -> Option<RwLockReadGuard<Option<Page>>> {
    self.page_map.get(page_id).and_then(| (page, page_index) | {
      let bitmap_cell = &self.read_indicator[page_index / BITMAP_CELL_SIZE];
      let mut bitmap_cell_value = bitmap_cell.load(Ordering::Relaxed);

      for _ in 0..10 {
        let new_cell_value = bitmap_cell_value | (1 << (page_index % BITMAP_CELL_SIZE));

        if let Err(real_value) = bitmap_cell.compare_exchange(bitmap_cell_value, new_cell_value, Ordering::Acquire, Ordering::Relaxed) {
            bitmap_cell_value = real_value;
        }
      }

      Some(page)
    })
  }

  pub fn add(&'a self, page: Page) -> Result<(), Page> {
    if let Err(mut page) = self.page_map.insert(page) {
      for _ in 0..10 {
        for _ in 0..5*self.size {
          let clock_page_index = self.clock.fetch_add(1, Ordering::Relaxed);
          let bitmap_cell = &self.read_indicator[clock_page_index / BITMAP_CELL_SIZE];
          let bitmap_cell_value = bitmap_cell.load(Ordering::Relaxed);
          let page_access_indicator = bitmap_cell_value | (1 << (clock_page_index % BITMAP_CELL_SIZE));

          if (bitmap_cell_value & page_access_indicator) != page_access_indicator {
            let _ = self.page_map.delete_by_index(clock_page_index);
            break;
          } else {
            let new_bitmap_cell_value = bitmap_cell_value & !(1 << (clock_page_index % BITMAP_CELL_SIZE));
            let _ = bitmap_cell.compare_exchange(bitmap_cell_value, new_bitmap_cell_value, Ordering::Acquire, Ordering::Relaxed);
          }
        }

        match self.page_map.insert(page) {
          Ok(_) => return Ok(()),
          Err(failed_page) => {
            page = failed_page;
          }
        };
      }

      return Err(page);
    } else {
      return Ok(());
    }
  }
}
