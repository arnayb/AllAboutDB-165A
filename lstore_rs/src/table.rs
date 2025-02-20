use crate::index::Index;
use crate::page::Page;
use std::collections::HashMap;

pub struct Record {
    pub rid: i64,
    pub key: i64,
    pub columns: Vec<i64>,
}

impl Record {
    pub fn new(rid: i64, key: i64, columns: Vec<i64>) -> Self {
        Record {
            rid,
            key,
            columns,
        }
    }
}
 
pub struct Table<'a> {
    num_columns: usize,
    name: String,
    key: usize,
    base_pages: Vec<Option<&'a Page>>, // Indices pointing to most recent tail pages (per column)
    tail_pages: Vec<Vec<Page>>,     // Each column has its own version history
    rid_counter: usize,
    pub index: Index,
    page_directory: HashMap<usize, Page>, // Maps page IDs to Page objects
}

impl<'a> Table<'a> {
    pub fn new(num_columns: usize, name: String, key: usize) -> Self {
        Table {
            num_columns,
            name,
            key,
            base_pages: vec![None; num_columns],  // No pages initially
            tail_pages: (0..num_columns).map(|_| vec![Page::new()]).collect(), // No pages initially
            rid_counter: 0,
            index: Index::new(num_columns, key),
            page_directory: HashMap::new(),
        }
    }
}
