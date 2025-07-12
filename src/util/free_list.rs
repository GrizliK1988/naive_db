use std::{
    cell::UnsafeCell,
    ptr::NonNull,
    sync::atomic::{AtomicPtr, Ordering},
};

use crate::page::Page;

const RETRIES: usize = 100;

#[derive(Debug)]
pub struct AllocatedPage<'a> {
    pub page: &'a mut Page,
    pub free_list_id: usize,
}

#[derive(Debug)]
pub struct ConcurrentFreeList<'a> {
    pub next: AtomicPtr<ConcurrentFreeListSlot<'a>>,
    pages: Vec<UnsafeCell<Page>>,
}

#[derive(Debug)]
pub struct ConcurrentFreeListSlot<'a> {
    pub value: usize,
    pub next: AtomicPtr<ConcurrentFreeListSlot<'a>>,
}

unsafe impl<'a> Sync for ConcurrentFreeList<'a> {}
unsafe impl<'a> Sync for ConcurrentFreeListSlot<'a> {}

impl<'a> ConcurrentFreeList<'a> {
    pub fn new(elements: Vec<usize>) -> Self {
        let Some(last_element) = elements.last() else {
            return Self {
                next: AtomicPtr::new(std::ptr::null_mut()),
                pages: vec![],
            };
        };

        let mut prev_slot = Box::into_raw(Box::new(ConcurrentFreeListSlot {
            value: *last_element,
            next: AtomicPtr::new(std::ptr::null_mut()),
        }));
        for element in elements.iter().rev().skip(1) {
            let slot = Box::into_raw(Box::new(ConcurrentFreeListSlot {
                value: *element,
                next: AtomicPtr::new(prev_slot),
            }));

            prev_slot = slot;
        }

        Self {
            next: AtomicPtr::new(prev_slot),
            pages: {
                let mut pages = Vec::with_capacity(elements.len());
                for _ in 0..elements.len() {
                    pages.push(UnsafeCell::new(Page::new(0)));
                }
                pages
            },
        }
    }

    pub fn allocate_page(&self) -> Result<AllocatedPage, ()> {
        for _ in 0..RETRIES {
            let Some(next) = NonNull::new(self.next.load(Ordering::Acquire)) else {
                return Err(());
            };

            let next_ptr = next.as_ptr();
            let new_next_ptr = unsafe { next.as_ref() }.next.load(Ordering::Acquire);

            if let Ok(next_ptr) = self.next.compare_exchange(
                next_ptr,
                new_next_ptr,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                let next = unsafe { Box::from_raw(next_ptr) };

                return Ok(AllocatedPage {
                    page: unsafe { &mut *self.pages[next.value].get() },
                    free_list_id: next.value,
                });
            }
        }

        Err(())
    }

    pub fn deallocate_page(&self, page: AllocatedPage<'a>) {
        loop {
            let next_ptr = self.next.load(Ordering::Acquire);

            let new_next = Box::into_raw(Box::new(ConcurrentFreeListSlot {
                value: page.free_list_id,
                next: AtomicPtr::new(next_ptr.clone()),
            }));

            if let Ok(_) =
                self.next
                    .compare_exchange(next_ptr, new_next, Ordering::Release, Ordering::Relaxed)
            {
                break;
            }
        }
    }
}
