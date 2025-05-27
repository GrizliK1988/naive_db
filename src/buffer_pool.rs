use crate::{hash_map::LinearIndirectPageHashMap, page::{Page, PageId}};

pub struct BufferPool {
    size: usize,
    pub page_map: LinearIndirectPageHashMap,
}

impl BufferPool {
  pub fn new(size: usize) -> BufferPool {
    BufferPool {
      size,
      page_map: LinearIndirectPageHashMap::new(size),
    } 
  }

  pub fn get(&self, page_id: PageId) -> Option<&Page> {
    self.page_map.get(page_id)
  }

  pub fn add(&self, page: Page) {
    match self.page_map.insert(page) {
        Ok(_) => {},
        Err(_) => {
          // self.page_map.insert(page);
        }
    }

    // match self.page_id_pool.len() < self.size {
    //     true => {
    //       let id = page.id;

    //       self.page_id_pool.push_back(page.id);
    //       self.page_map.insert(id, page);
    //     },
    //     false => {
    //       let pi = self.page_id_pool.pop_front().unwrap();
    //       self.page_map.remove(&pi);

    //       self.page_id_pool.push_back(page.id);
    //       self.page_map.insert(page.id, page);
    //     }
    // };
  }
}
