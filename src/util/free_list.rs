use std::{ptr::NonNull, sync::atomic::{AtomicPtr, Ordering}};

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
pub struct ConcurrentFreeList {
    pub value: Option<usize>,
    pub next: AtomicPtr<ConcurrentFreeList>
}

impl ConcurrentFreeList {
    pub fn new(elements: Vec<usize>) -> Self {
        let Some(last_element) = elements.last() else {
            return Self {
                value: None,
                next: AtomicPtr::new(std::ptr::null_mut())
            };
        };

        let mut prev_slot = Box::into_raw(Box::new(Self {
            value: Some(*last_element),
            next: AtomicPtr::new(std::ptr::null_mut())
        }));
        for element in elements.iter().rev().skip(1) {
            let slot = Box::into_raw(Box::new(Self {
                value: Some(*element),
                next: AtomicPtr::new(prev_slot)
            }));

            prev_slot = slot;
        }

        Self {
            value: None,
            next: AtomicPtr::new(prev_slot)
        }
    }

    pub fn allocate(&self) -> Result<usize, ()> {
        for _ in 0..100 {
            let Some(next) = NonNull::new(self.next.load(Ordering::Acquire)) else {
                return Err(())
            };

            let next_ptr = next.as_ptr();
            let new_next_ptr = unsafe { next.as_ref() }.next.load(Ordering::Acquire);

            if let Ok(next_ptr) = self.next.compare_exchange(next_ptr, new_next_ptr, Ordering::Release, Ordering::Relaxed) {
                let next = unsafe { &*next_ptr };

                return Ok(next.value.unwrap())
            }
        }

        Err(())
    }

    pub fn deallocate(&self, element: &usize) -> Result<(), ()> {
        for _ in 0..100 {
            let next_ptr = self.next.load(Ordering::Acquire);

            let new_next = Box::into_raw(Box::new(Self {
                value: Some(element.to_owned()),
                next: AtomicPtr::new(next_ptr.clone()),
            }));

            if let Ok(_) = self.next.compare_exchange(next_ptr, new_next, Ordering::Release, Ordering::Relaxed) {
                return Ok(())
            }
        }

        Err(())
    }
}
