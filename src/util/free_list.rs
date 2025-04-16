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