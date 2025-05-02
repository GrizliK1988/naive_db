use std::sync::Arc;

use twox_hash::XxHash3_64;

use crate::page::{Page, PageId};

#[derive(Clone, Default)]
struct KeyOrThumbstone {
    page_id: PageId,
    page_index: usize,
    is_thumbstone: bool,
}

impl KeyOrThumbstone {
    fn mark_thumbstone(&mut self) {
        self.is_thumbstone = true;
    }
}

pub struct LinearPageHashMap {
    size: usize,
    free_slots: usize,
    page_keys: Vec<Option<KeyOrThumbstone>>,
    pages: Vec<Arc<Page>>
}

impl LinearPageHashMap {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            free_slots: size,
            page_keys: vec![Default::default(); size * 2],
            pages: Vec::with_capacity(size),
        }
    }

    pub fn insert(&mut self, page: Page) -> Result<(), ()> {
        if self.free_slots == 0 {
            return Err(())
        }

        let hash = XxHash3_64::oneshot(&page.id.to_be_bytes()) as usize;
        let key = hash % self.size;

        let page_id = page.id;

        let mut k = key;
        let keys_size = self.size * 2;

        'main_loop: loop {
            let k_idx = k % keys_size;

            match &self.page_keys[k_idx] {
                Some(existing_key) => {
                    if existing_key.is_thumbstone || existing_key.page_id == page_id {
                        self.pages[existing_key.page_index] = Arc::new(page);
                        self.free_slots -= 1;

                        self.page_keys[k_idx] = Some(KeyOrThumbstone {
                            page_id,
                            is_thumbstone: false,
                            page_index: existing_key.page_index,
                        });
                        break 'main_loop Ok(())
                    }

                    k += 1;

                    if k == key + keys_size {
                        break 'main_loop Err(())
                    }
                },
                None => {
                    self.pages.push(Arc::new(page));
                    self.free_slots -= 1;

                    self.page_keys[k_idx] = Some(KeyOrThumbstone {
                        page_id,
                        is_thumbstone: false,
                        page_index: self.pages.len() - 1,
                    });
                    break 'main_loop Ok(())
                }
            };
        }
    }

    pub fn delete(&mut self, page_id: &PageId) -> Result<(), ()> {
        let hash = XxHash3_64::oneshot(&page_id.to_be_bytes()) as usize;
        let key = hash % self.size;

        let mut k = key;
        let keys_size = self.size * 2;

        loop {
            let page_key = self.page_keys[k % keys_size].as_mut();

            match page_key {
                Some(page_key) => {
                    if &page_key.page_id == page_id {
                        page_key.mark_thumbstone();
                        self.free_slots += 1;

                        break Ok(())
                    }

                    k += 1;

                    if k == key + keys_size {
                        break Err(())
                    }
                },
                None => break Ok(()),
            }
        }
    }

    pub fn get(&self, page_id: PageId) -> Option<Arc<Page>> {
        let hash = XxHash3_64::oneshot(&page_id.to_be_bytes()) as usize;
        let key = hash % self.size;

        let mut k = key;
        let keys_size = self.size * 2;

        loop {
            let page = self.page_keys[k % keys_size]
                    .as_ref()
                    .and_then(| page_key | {
                        match page_key.is_thumbstone {
                            true => None,
                            false => Some(self.pages[page_key.page_index].clone()),
                        }
                    });

            match page {
                Some(page) => {
                    if page.id == page_id {
                        break Some(page)
                    }

                    k += 1;

                    if k == key + keys_size {
                        break None
                    }
                },
                None => break None,
            }
        }
    }
}