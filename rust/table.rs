// mod index;

use index::Index;
use std::collections::HashMap;

const RID_COLUMN: isize = -2;
const INDIRECTION_COLUMN: isize = -3;
const TIMESTAMP_COLUMN: isize = -4;
const SCHEMA_ENCODING_COLUMN: isize = -5;

pub struct Record {
    rid: usize,
    key: i32,
    columns: Vec<i32>,
}

impl Record {
    pub fn new(rid: usize, key: i32, columns: Vec<i32>) -> Self {
        Record {
            rid,
            key,
            columns,
        }
    }
}
 
pub struct Table {
    num_columns: usize,
    name: String,
    key: i32,
    base_pages: Vec<Option<usize>>, // Indices pointing to most recent tail pages (per column)
    tail_pages: Vec<Vec<Page>>,     // Each column has its own version history
    rid_counter: usize,
    index: Index,
    page_directory: HashMap<usize, Page>, // Maps page IDs to Page objects
}

impl Table {
    pub fn new(num_columns: usize, name: String, key: i32) -> Self {
        Table {
            num_columns,
            name,
            key,
            base_pages: vec![None; num_columns],  // No pages initially
            tail_pages: vec![Vec::new(); num_columns], // Each column gets its own versioned history
            rid_counter: 0,
            index: Index::new(),
            page_directory: HashMap::new(),
        }
    }
}
