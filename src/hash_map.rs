use std::{cell::UnsafeCell, sync::{atomic::{AtomicUsize}, RwLock, RwLockWriteGuard}};

use twox_hash::XxHash3_64;

use crate::{page::{Page, PageId}, util::free_list::ConcurrentFreeList};

#[derive(Debug)]
pub struct KeyOrThumbstone {
    page_id: PageId,
    page_index: usize,
    is_thumbstone: bool,
}

impl KeyOrThumbstone {
    fn mark_thumbstone(&mut self) {
        self.is_thumbstone = true;
    }
}

#[derive(Debug)]
pub struct LinearPageHashMap {
    size: usize,
    free_list: ConcurrentFreeList,
    pub page_keys: Vec<RwLock<Option<KeyOrThumbstone>>>,
    pub pages: UnsafeCell<Vec<Option<Page>>>,
    clock_page_index: AtomicUsize
}

unsafe impl Sync for LinearPageHashMap {}

impl LinearPageHashMap {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            free_list: ConcurrentFreeList::new((0..size).collect()),
            page_keys: (0..size*2).into_iter().map(|_| RwLock::new(None)).collect(),
            pages: UnsafeCell::new((0..size).into_iter().map(|_| None).collect()),
            clock_page_index: AtomicUsize::new(0),
        }
    }

    pub fn insert<'a>(&'a self, page: Page) -> Result<(), ()> {
        let Ok(allocated_slot) = self.free_list.allocate() else {
            return Err(())
        };

        let hash = XxHash3_64::oneshot(&page.id.to_be_bytes()) as usize;
        let key = hash % self.size;

        let mut k = key;
        let keys_size = self.size * 2;

        let mut page_key_write: Option<RwLockWriteGuard<'a, Option<KeyOrThumbstone>>> = None;

        let result = 'main_loop: loop {
            let k_idx = k % keys_size;

            let can_write: bool = {
                let page_key_read = &*self.page_keys[k_idx].read().unwrap();

                match page_key_read {
                    Some(existing_key) => existing_key.is_thumbstone || &existing_key.page_id == &page.id,
                    None => true,
                }
            };

            if can_write {
                match self.prepare_store_page(page.id, k_idx, &mut page_key_write) {
                    Ok(_) => break 'main_loop Ok(()),
                    Err(_) => {},
                };
            }

            k += 1;

            if k == key + keys_size {
                break 'main_loop Err(())
            }
        };

        match result {
            Ok(_) => {
                self.store_page(allocated_slot, page, page_key_write.unwrap());
                Ok(())
            },
            Err(_) => Err(())
        }
    }

    fn prepare_store_page<'a>(&'a self, page_id: PageId, page_key_index: usize, page_write: &mut Option<RwLockWriteGuard<'a, Option<KeyOrThumbstone>>>) -> Result<(), ()> {
        let page_key_write = self.page_keys[page_key_index].write().unwrap();

        match &*page_key_write {
            Some(page_key_write) if !page_key_write.is_thumbstone && page_key_write.page_id != page_id => Err(()),
            _ => {
                page_write.replace(page_key_write);
                Ok(())
            }
        }
    }

    fn store_page<'a>(&self, allocated_slot: usize, page: Page, mut page_key_write: RwLockWriteGuard<'a, Option<KeyOrThumbstone>>) {
        let pages = unsafe {
            &mut *self.pages.get()
        };
        let page_id = page.id;

        pages[allocated_slot] = Some(page);
        *page_key_write = Some(KeyOrThumbstone {
            page_id,
            is_thumbstone: false,
            page_index: allocated_slot,
        });
    }

    pub fn delete(&self, page_id: &PageId) -> Result<(), ()> {
        let hash = XxHash3_64::oneshot(&page_id.to_be_bytes()) as usize;
        let key = hash % self.size;

        let mut k = key;
        let keys_size = self.size * 2;

        loop {
            let k_idx = k % keys_size;

            let can_delete = {
                let page_key_read = &*self.page_keys[k_idx].read().unwrap();
                match page_key_read {
                    Some(page_key) => {
                        if &page_key.page_id == page_id {
                            Ok((true, true))
                        } else {
                            k += 1;
    
                            if k == key + keys_size {
                                Err(())
                            } else {
                                Ok((false, false))
                            }
                        }
                    },
                    None => Ok((false, true)),
                }
            };

            match can_delete {
                Ok((true, _)) => {
                    let mut page_key_write = self.page_keys[k_idx].write().unwrap();
                    (*page_key_write).as_mut().unwrap().mark_thumbstone();

                    let page_index = (*page_key_write).as_ref().unwrap().page_index;
                    self.free_list.deallocate(&page_index)?;

                    break Ok(())
                },
                Ok((false, true)) => break Ok(()),
                Ok((false, false)) => {},
                Err(()) => break Err(()),
            };
        }
    }

    pub fn get(&self, page_id: PageId) -> Option<&Page> {
        let hash = XxHash3_64::oneshot(&page_id.to_be_bytes()) as usize;
        let key = hash % self.size;

        let mut k = key;
        let keys_size = self.size * 2;

        let pages = unsafe {
            &mut *self.pages.get()
        };

        loop {
            let page_index = {
                let page_key_read = self.page_keys[k % keys_size].read().unwrap();
                match &*page_key_read {
                    Some(page_key) if !page_key.is_thumbstone && page_key.page_id == page_id => page_key.page_index,
                    Some(page_key) if page_key.is_thumbstone || page_key.page_id != page_id => {
                        k += 1;

                        if k == key + keys_size {
                            break None
                        }

                        continue
                    },
                    _ => break None,
                }
            };

            match (&pages[page_index]).as_ref() {
                Some(page) if page.id == page_id => break Some(&page),
                _ => break None,
            }
        }
    }
}