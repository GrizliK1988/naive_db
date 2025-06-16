use std::{cell::UnsafeCell, ptr::NonNull, sync::atomic::{AtomicPtr, Ordering}};

use crate::page::Page;

const RETRIES: usize = 100;

#[derive(Clone, PartialEq, Debug)]
pub struct FreeList {
    pub value: Option<usize>,
    pub next: Option<Box<FreeList>>
}

impl FreeList {
    pub fn new(elements: Vec<usize>) -> FreeList {
        elements
            .iter()
            .rev()
            .fold(
                FreeList {
                value: None,
                next: None
                },
                | acc, &element | {
                    match acc.value {
                        Some(_) => FreeList {
                            value: Some(element),
                            next: Some(Box::from(acc))
                        },
                        None => FreeList {
                            value: Some(element),
                            next: None
                        }
                    }
                }
            )
    }

    pub fn add(&mut self, element: usize) {
        let mut new_next = FreeList {
            value: None,
            next: None,
        };
        
        std::mem::swap(self, &mut new_next);

        *self = FreeList {
            value: Some(element),
            next: Some(Box::from(new_next)),
        };
    }

    pub fn release(&mut self) -> Option<usize> {
        let released_value = self.value.take();

        if let Some(mut n) = self.next.take() {
            std::mem::swap(self, &mut n);
        }

        released_value
    }
}

#[derive(Debug)]
pub struct AllocatedPage<'a> {
    pub page: &'a mut Page,
    pub free_list_id: usize,
}

#[derive(Debug)]
pub struct ConcurrentFreeList<'a> {
    pub next: AtomicPtr<ConcurrentFreeListSlot<'a>>,
    size: usize,
    pages: Vec<UnsafeCell<Page>>
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
                size: 0,
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
                next: AtomicPtr::new(prev_slot)
            }));

            prev_slot = slot;
        }

        Self {
            next: AtomicPtr::new(prev_slot),
            size: elements.len(),
            pages: {
                let mut pages = Vec::with_capacity(elements.len());
                for _ in 0..elements.len() {
                    pages.push(UnsafeCell::new(Page::new(0)));
                }
                pages
            }
        }
    }

    pub fn allocate_page(&self) -> Result<AllocatedPage, ()> {
        for _ in 0..RETRIES {
            let Some(next) = NonNull::new(self.next.load(Ordering::Acquire)) else {
                return Err(())
            };

            let next_ptr = next.as_ptr();
            let new_next_ptr = unsafe { next.as_ref() }.next.load(Ordering::Acquire);

            if let Ok(next_ptr) = self.next.compare_exchange(next_ptr, new_next_ptr, Ordering::Release, Ordering::Relaxed) {
                let next = unsafe { Box::from_raw(next_ptr) };

                return Ok(AllocatedPage { page: unsafe {&mut *self.pages[next.value].get()}, free_list_id: next.value })
            }
        }

        Err(())
    }

    pub fn allocate(&self) -> Result<usize, ()> {
        for _ in 0..RETRIES {
            let Some(next) = NonNull::new(self.next.load(Ordering::Acquire)) else {
                return Err(())
            };

            let next_ptr = next.as_ptr();
            let new_next_ptr = unsafe { next.as_ref() }.next.load(Ordering::Acquire);

            if let Ok(next_ptr) = self.next.compare_exchange(next_ptr, new_next_ptr, Ordering::Release, Ordering::Relaxed) {
                let next = unsafe { &*next_ptr };

                return Ok(next.value)
            }
        }

        Err(())
    }

    pub fn deallocate_page(&self, page: AllocatedPage<'a>) -> Result<(), AllocatedPage<'a>> {
        for _ in 0..RETRIES {
            let next_ptr = self.next.load(Ordering::Acquire);

            let new_next = Box::into_raw(Box::new(ConcurrentFreeListSlot {
                value: page.free_list_id,
                next: AtomicPtr::new(next_ptr.clone()),
            }));

            if let Ok(_) = self.next.compare_exchange(next_ptr, new_next, Ordering::Release, Ordering::Relaxed) {
                return Ok(())
            }
        }

        Err(page)
    }

    pub fn deallocate(&self, element: &usize) -> Result<(), ()> {
        for _ in 0..RETRIES {
            let next_ptr = self.next.load(Ordering::Acquire);

            let new_next = Box::into_raw(Box::new(ConcurrentFreeListSlot {
                value: element.to_owned(),
                next: AtomicPtr::new(next_ptr.clone()),
            }));

            if let Ok(_) = self.next.compare_exchange(next_ptr, new_next, Ordering::Release, Ordering::Relaxed) {
                return Ok(())
            }
        }

        Err(())
    }
}
