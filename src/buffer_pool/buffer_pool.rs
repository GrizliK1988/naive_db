use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard};

use crate::{
    buffer_pool::page_hash_map::{BufferPoolPageHashMap, InsertPageResult},
    page::{Page, PageId},
    persist::Reader,
};

pub enum BufferPoolPage<'a> {
    PageFromPool(MappedRwLockReadGuard<'a, Page>),
    PageFromDisk(MappedRwLockWriteGuard<'a, Page>),
}

pub struct ReadPageGuard<'a> {
    buffer_pool_page: BufferPoolPage<'a>,
}

impl<'a> ReadPageGuard<'a> {
    fn new_page_from_pool(page_guard: MappedRwLockReadGuard<'a, Page>) -> Self {
        Self {
            buffer_pool_page: BufferPoolPage::PageFromPool(page_guard),
        }
    }

    fn new_page_from_disk(page_guard: MappedRwLockWriteGuard<'a, Page>) -> Self {
        Self {
            buffer_pool_page: BufferPoolPage::PageFromDisk(page_guard),
        }
    }

    pub fn get(&'a self) -> &'a Page {
        match &self.buffer_pool_page {
            BufferPoolPage::PageFromPool(page) => &*page,
            BufferPoolPage::PageFromDisk(page) => &*page,
        }
    }
}

pub struct BufferPool<'a> {
    page_map: BufferPoolPageHashMap<'a>,
    reader: Reader,
}

impl<'a> BufferPool<'a> {
    pub fn new(size: usize, reader: Reader) -> BufferPool<'a> {
        BufferPool {
            page_map: BufferPoolPageHashMap::new(size),
            reader,
        }
    }

    pub fn get(&'a self, page_id: PageId) -> Result<ReadPageGuard<'a>, ()> {
        if let Some(read_guard) = self.page_map.read_page(&page_id) {
            return Ok(ReadPageGuard::new_page_from_pool(read_guard));
        }

        let Ok(insert_result) = self.page_map.insert_page(&page_id) else {
            return Err(());
        };

        match insert_result {
            InsertPageResult::ExistingPage(guard) => Ok(ReadPageGuard::new_page_from_pool(guard)),
            InsertPageResult::NewPage(mut write_guard) => {
                self.reader.read_page(page_id, &mut *write_guard)?;
                write_guard.id = page_id;
                write_guard.refresh_metadata();

                Ok(ReadPageGuard::new_page_from_disk(write_guard))
            }
        }
    }
}
