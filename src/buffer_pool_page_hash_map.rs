use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use twox_hash::XxHash3_64;
use crate::{page::PageId, util::free_list::{AllocatedPage, ConcurrentFreeList}};

#[derive(Debug)]
pub struct KeyOrThumbstone<'a> {
    allocated_page: AllocatedPage<'a>,
    is_thumbstone: bool,
}

impl<'a> KeyOrThumbstone<'a> {
    fn mark_thumbstone(&mut self) {
        self.is_thumbstone = true;
    }

    fn page_id(&self) -> &PageId {
        &self.allocated_page.page.id
    }
}

#[derive(Debug)]
pub struct BufferPoolPageHashMap<'a> {
    size: usize,
    free_list: ConcurrentFreeList<'a>,
    pub page_keys: Vec<RwLock<Option<KeyOrThumbstone<'a>>>>,
}

// unsafe impl<'a> Sync for BufferPoolPageHashMap<'a> {}

impl<'a> BufferPoolPageHashMap<'a> {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            free_list: ConcurrentFreeList::new((0..size).collect()),
            page_keys: (0..size*2).into_iter().map(|_| RwLock::new(None)).collect(),
        }
    }

    pub fn get_page_for_writing(&'a self, page_id: &PageId) -> Result<MappedRwLockWriteGuard<'a, AllocatedPage<'a>>, ()> {
        let Ok(allocated_page) = self.free_list.allocate_page() else {
            return Err(())
        };

        let hash = XxHash3_64::oneshot(&page_id.to_be_bytes()) as usize;
        let key = hash % self.size;

        let mut k = key;
        let keys_size = self.size * 2;

        let page_key_write = loop {
            let k_idx = k % keys_size;

            let key_read_guard = self.page_keys[k_idx].upgradable_read();

            match &*key_read_guard {
                Some(page_key) if !page_key.is_thumbstone && page_key.page_id() != page_id => {
                    k += 1;

                    if k == key + keys_size {
                        break Err(())
                    }

                    continue;
                },
                _ => {
                    let Ok(write_lock) = RwLockUpgradableReadGuard::try_upgrade(key_read_guard) else {
                        k += 1;

                        if k == key + keys_size {
                            break Err(())
                        }

                        continue;
                    };

                    break Ok(write_lock)
                }
            };
        };

        match page_key_write {
            Ok(mut guard) => {
                *guard = Some(KeyOrThumbstone {
                    allocated_page,
                    is_thumbstone: false,
                });

                Ok(RwLockWriteGuard::map(guard,| x | &mut x.as_mut().unwrap().allocated_page))
            },
            Err(_) => Err(())
        }
    }

    pub fn get(&self, page_id: &PageId) -> Option<MappedRwLockReadGuard<AllocatedPage>> {
        let hash = XxHash3_64::oneshot(&page_id.to_be_bytes()) as usize;
        let key = hash % self.size;

        let mut k = key;
        let keys_size = self.size * 2;

        loop {
            let k_idx = k % keys_size;

            let key_read_guard = self.page_keys[k_idx].read();

            match &*key_read_guard {
                Some(page_key) if !page_key.is_thumbstone && page_key.page_id() == page_id => {
                    break Some(RwLockReadGuard::map(key_read_guard, | x | &x.as_ref().unwrap().allocated_page))
                },
                Some(page_key) if !page_key.is_thumbstone && page_key.page_id() != page_id => {
                    k += 1;

                    if k == key + keys_size {
                        break None
                    }

                    continue;
                },
                _ => break None,
            };
        }
    }
}