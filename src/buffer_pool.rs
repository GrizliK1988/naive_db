use std::sync::{atomic::{AtomicU8, AtomicUsize, Ordering}, RwLockReadGuard};

use crate::{hash_map::LinearIndirectPageHashMap, page::{Page, PageId}};

const BITMAP_CELL_SIZE: usize = 8;

pub struct BufferPool {
    pub page_map: LinearIndirectPageHashMap,
    size: usize,
    clock: AtomicUsize,
    read_indicator: Vec<AtomicU8>,
}

impl BufferPool {
  pub fn new(size: usize) -> BufferPool {
    BufferPool {
      size,
      page_map: LinearIndirectPageHashMap::new(size),
      clock: AtomicUsize::new(0),
      read_indicator: (0..size/BITMAP_CELL_SIZE+1).into_iter().map(|_| AtomicU8::new(0)).collect()
    } 
  }

  pub fn get(&self, page_id: PageId) -> Option<RwLockReadGuard<Option<Page>>> {
    self.page_map.get(page_id).and_then(| (page, page_index) | {
      let mut i = 0;
      let bitmap_cell = &self.read_indicator[page_index / BITMAP_CELL_SIZE];
      let mut bitmap_cell_value = bitmap_cell.load(Ordering::Relaxed);
      loop {
        let new_cell_value = bitmap_cell_value | (1 << (page_index % BITMAP_CELL_SIZE));

        let result = bitmap_cell.compare_exchange(bitmap_cell_value, new_cell_value, Ordering::Acquire, Ordering::Relaxed);
        match result {
          Ok(_) => break,
          Err(real_value) => {
            bitmap_cell_value = real_value;

            i += 1;
            if i > 10 {
              break;
            }
          }
        }
      }

      Some(page)
    })
  }

  pub fn add(&self, page: Page) -> Result<(), Page> {
    match self.page_map.insert(page) {
        Ok(_) => Ok(()),
        Err(failed_page) => {
          let mut i = 0;

          let remove_result = loop {
              let clock_page_index = self.clock.fetch_add(1, Ordering::Relaxed);
              let bitmap_cell = &self.read_indicator[clock_page_index / BITMAP_CELL_SIZE];
              let bitmap_cell_value = bitmap_cell.load(Ordering::Relaxed);
              let page_access_indicator = bitmap_cell_value | (1 << (clock_page_index % BITMAP_CELL_SIZE));

              if (bitmap_cell_value & page_access_indicator) != page_access_indicator {
                break self.page_map.delete_by_index(clock_page_index);
              } else {
                let new_bitmap_cell_value = bitmap_cell_value & !(1 << (clock_page_index % BITMAP_CELL_SIZE));
                let _ = bitmap_cell.compare_exchange(bitmap_cell_value, new_bitmap_cell_value, Ordering::Acquire, Ordering::Relaxed);
              }

              i += 1;
              if i > self.size * 5 {
                break Err(());
              }
          };

          if remove_result.is_ok() {
            self.page_map.insert(failed_page)?;
            Ok(())
          } else {
            Err(failed_page)
          }
        }
    }
  }
}
