use crate::constants::PAGE_SIZE;
use crate::table::Record;

pub struct Page {
    num_records: i64,
    pub data: Vec<Option<Record>>,
    curr_capacity: i64
}

impl Page {
    pub fn new() -> Self {
        Page {
            num_records: 0,
            data: Vec::new(),
            curr_capacity: 0
        }
    }

    pub fn has_capacity(&self, record: &Record) -> bool {
        let record_size = 8 + 8 + (record.columns.len() as i64 * 8);
        self.curr_capacity + record_size <= PAGE_SIZE
    }

    pub fn insert(&mut self, record: Record) -> bool {
        if !self.has_capacity(&record) {
            return false;
        }

        let record_size = 8 + 8 + (record.columns.len() as i64 * 8);
        self.curr_capacity += record_size;
        self.data.push(Some(record));
        self.num_records += 1;
        return true
    }
}
