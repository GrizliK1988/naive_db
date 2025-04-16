use std::collections::{HashMap, VecDeque};

use crate::page::{Page, PageId};

pub struct BufferPool {
    size: usize,
    page_id_pool: VecDeque<PageId>,
    pub page_map: HashMap<PageId, Page>,
}

impl BufferPool {
  pub fn new(size: usize) -> BufferPool {
    BufferPool {
      size,
      page_id_pool: VecDeque::with_capacity(size),
      page_map: HashMap::with_capacity(size),
    } 
  }

  pub fn get(&self, page_id: PageId) -> Option<&Page> {
    self.page_map.get(&page_id)
  }

  pub fn add(&mut self, page: Page) {
    match self.page_id_pool.len() < self.size {
        true => {
          let id = page.id;

          self.page_id_pool.push_back(page.id);
          self.page_map.insert(id, page);
        },
        false => {
          let pi = self.page_id_pool.pop_front().unwrap();
          self.page_map.remove(&pi);

          self.page_id_pool.push_back(page.id);
          self.page_map.insert(page.id, page);
        }
    };
  }
}
