use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use twox_hash::XxHash3_64;
use crate::{page::{Page, PageId}, util::free_list::{AllocatedPage, ConcurrentFreeList}};

#[derive(Debug)]
pub struct Entry<'a> {
    allocated_page: Option<AllocatedPage<'a>>,
}

impl<'a> Entry<'a> {
    fn page_id(&self) -> Option<&PageId> {
        let Some(allocated_page) = self.allocated_page.as_ref() else {
            return None
        };

        Some(&allocated_page.page.id)
    }
    
    fn is_thumbstone(&self) -> bool {
        self.allocated_page.is_none()
    }
}

pub enum InsertPageResult<'a> {
    NewPage(MappedRwLockWriteGuard<'a, Page>),
    ExistingPage(MappedRwLockReadGuard<'a, Page>),
}

pub enum InsertPageResultInternal<'a> {
    NewPage(RwLockWriteGuard<'a, Option<Entry<'a>>>),
    ExistingPage(RwLockReadGuard<'a, Option<Entry<'a>>>),
}

#[derive(Debug)]
pub enum InsertPageError {
    NoFreeSlot,
    FailedToInsert,
}

#[derive(Debug)]
pub struct BufferPoolPageHashMap<'a> {
    size: usize,
    free_list: ConcurrentFreeList<'a>,
    pub page_keys: Vec<RwLock<Option<Entry<'a>>>>,
}

impl<'a> BufferPoolPageHashMap<'a> {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            free_list: ConcurrentFreeList::new((0..size).collect()),
            page_keys: (0..size*2).into_iter().map(|_| RwLock::new(None)).collect(),
        }
    }

    pub fn insert_page(&'a self, page_id: &PageId) -> Result<InsertPageResult<'a>, InsertPageError> {
        let Ok(allocated_page) = self.free_list.allocate_page() else {
            return Err(InsertPageError::NoFreeSlot)
        };

        let insert_result = self.try_insert_page(page_id);

        match insert_result {
            Ok(InsertPageResultInternal::NewPage(mut guard)) => {
                *guard = Some(Entry {
                    allocated_page: Some(allocated_page),
                });

                let locked_page = RwLockWriteGuard::map(guard,| x | x.as_mut().unwrap().allocated_page.as_mut().unwrap().page);
                Ok(InsertPageResult::NewPage(locked_page))
            },
            Ok(InsertPageResultInternal::ExistingPage(guard)) => {
                let _ = self.free_list.deallocate(&allocated_page.free_list_id);
                let locked_page = RwLockReadGuard::map(guard,| x | x.as_ref().unwrap().allocated_page.as_ref().unwrap().page);
                Ok(InsertPageResult::ExistingPage(locked_page))
            },
            Err(_) => Err(InsertPageError::FailedToInsert)
        }
    }

    fn try_insert_page(&'a self, page_id: &PageId) -> Result<InsertPageResultInternal<'a>, ()> {
        let hash = XxHash3_64::oneshot(&page_id.to_be_bytes()) as usize;
        let key = hash % self.size;

        let keys_size = self.size * 2;
        let mut k = key;

        loop {
            let k_idx = k % keys_size;
            let key_read_guard = self.page_keys[k_idx].upgradable_read();
            let page_key = &*key_read_guard;

            if matches!(page_key, Some(page_key) if !page_key.is_thumbstone() && page_key.page_id().unwrap() != page_id) {
                k += 1;

                if k == key+keys_size {
                    return Err(())
                }

                continue;
            }

            if matches!(page_key, Some(page_key) if !page_key.is_thumbstone() && page_key.page_id().unwrap() == page_id) {
                return Ok(InsertPageResultInternal::ExistingPage(RwLockUpgradableReadGuard::downgrade(key_read_guard)))
            }

            let Ok(write_lock) = RwLockUpgradableReadGuard::try_upgrade(key_read_guard) else {
                continue;
            };

            return Ok(InsertPageResultInternal::NewPage(write_lock))
        };
    }

    pub fn read_page(&self, page_id: &PageId) -> Option<MappedRwLockReadGuard<Page>> {
        let hash = XxHash3_64::oneshot(&page_id.to_be_bytes()) as usize;
        let key = hash % self.size;

        let mut k = key;
        let keys_size = self.size * 2;

        loop {
            let k_idx = k % keys_size;

            let key_read_guard = self.page_keys[k_idx].read();

            match &*key_read_guard {
                Some(page_key) if !page_key.is_thumbstone() && page_key.page_id().unwrap() == page_id => {
                    break Some(RwLockReadGuard::map(key_read_guard, | x | &*x.as_ref().unwrap().allocated_page.as_ref().unwrap().page))
                },
                Some(page_key) if !page_key.is_thumbstone() && page_key.page_id().unwrap() != page_id => {
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