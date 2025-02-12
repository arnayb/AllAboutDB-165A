const PAGE_SIZE: usize = 4096; // 4KB pages

pub struct Page {
    num_records: usize,
    data: Vec<Option<i32>>,
}

impl Page {
    pub fn new() -> Self {
        Page {
            num_records: 0,
            data: vec![None; PAGE_SIZE]
        }
    }
    
    pub fn has_capacity(&self) -> bool {
        self.num_records < PAGE_SIZE
    }

    pub fn insert(&mut self, value: i32) {
        if self.has_capacity() {
            self.data[self.num_records] = Some(value);
            self.num_records += 1;
        } else {
            panic!("Page is full");
        }
    }
}