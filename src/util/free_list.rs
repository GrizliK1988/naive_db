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
        elements
            .iter()
            .rev()
            .fold(
                Self {
                    value: None,
                    next: AtomicPtr::new(std::ptr::null_mut())
                },
                | acc, &element | {
                    match NonNull::new(acc.next.load(Ordering::Relaxed)) {
                        Some(next) => {
                            let slot = Box::into_raw(Box::new(Self {
                                value: Some(element),
                                next: AtomicPtr::new(next.as_ptr())
                            }));

                            acc.next.store(slot, Ordering::Relaxed);

                            acc
                        },
                        None => {
                            let slot = Box::into_raw(Box::new(Self {
                                value: Some(element),
                                next: AtomicPtr::new(std::ptr::null_mut())
                            }));

                            acc.next.store(slot, Ordering::Relaxed);

                            acc
                        }
                    }
                }
            )
    }

    pub fn allocate(&self) -> Result<usize, ()> {
        let mut i = 0;

        loop {
            let next = NonNull::new(self.next.load(Ordering::Acquire));

            if let None = next {
                return Err(())
            }

            let next = next.unwrap();

            let next_ptr = next.as_ptr();
            let new_next_ptr = unsafe { next.as_ref() }.next.load(Ordering::Acquire);

            match self.next.compare_exchange(next_ptr, new_next_ptr, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(next_ptr) => {
                    let next = unsafe { &*next_ptr };

                    return Ok(next.value.unwrap())
                },
                Err(_) => {
                }
            }

            i+=1;
            if i > 100 {
                return Err(())
            }
            
            std::hint::spin_loop();
        }
    }

    pub fn deallocate(&self, element: &usize) -> Result<(), ()> {
        let mut i = 0;

        loop {
            let next_ptr = self.next.load(Ordering::Acquire);

            let new_next = Box::into_raw(Box::new(Self {
                value: Some(element.to_owned()),
                next: AtomicPtr::new(next_ptr.clone()),
            }));

            match self.next.compare_exchange(next_ptr, new_next, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => {
                    return Ok(())
                },
                Err(_) => {
                }
            }

            i+=1;
            if i > 100 {
                return Err(())
            }
            
            std::hint::spin_loop();
        }
    }
}
