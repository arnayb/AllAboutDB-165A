use crate::page::Page;
use std::collections::{BTreeMap, HashMap};

pub struct Index {
    num_columns: usize,
    key: usize,
    hash_indices: Vec<HashMap<usize, Vec<(usize, usize)>>>,
    btree_indices: Vec<BTreeMap<usize, usize>>,
}

impl Index {
    pub fn new(num_columns: usize, key: usize) -> Self {
        Self { 
            num_columns,
            key,
            hash_indices: vec![HashMap::new(); num_columns],
            btree_indices: vec![BTreeMap::new(); num_columns],
        }
    }

    /// Locate all records with the given value in the specified column
    pub fn locate(&self, column: usize, value: usize) -> Vec<usize> {
        if column >= self.num_columns {
            panic!("Invalid column number: {}", column);
        }
    
        let mut results = Vec::new();
    
        if let Some(bucket) = self.hash_indices[column].get(&value) {
            results.extend(bucket.iter().map(|(rid, _value)| *rid));
        }
        
        // Try B-tree index if hash index didn't find anything
        if results.is_empty() {
            if let Some(&rid) = self.btree_indices[column].get(&value) {
                results.push(rid);
            }
        }
    
        results
    }

    pub fn locate_range(&self, begin: usize, end: usize, column: usize) -> Vec<usize> {
        if column >= self.num_columns {
            panic!("Invalid column number: {}", column);
        }

        let (begin, end) = if begin > end {
            (end, begin)
        } else {
            (begin, end)
        };

        let mut results = Vec::new();

        // Try B-tree index first
        if let Some(btree_index) = self.btree_indices.get(column) { 
            results.extend(
            btree_index
                .range(begin..=end)
                .map(|(_value, &rid)| rid) 
            );
        }


        // Try hash index if B-tree index didn't find anything
        if results.is_empty() {
            for i in begin..=end {
                if let Some(bucket) = self.hash_indices[column].get(&i) {
                    results.extend(bucket.iter().map(|(rid, _value)| *rid));
                }
            }
        }
        results
    }

     /// Create index on specific column
     pub fn index_column(&mut self, column: usize, base_pages: &Vec<Option<&Page>>) {
        if column >= self.num_columns {
            panic!("Invalid column number: {}", column);
        }

        // Clear existing indices for this column
        self.hash_indices[column].clear();
        self.btree_indices[column].clear();

        // Get records from the page
        if let Some(page) = base_pages[column] {
            for (_i, record) in page.data.iter().enumerate() {
                if let Some(record) = record {
                    // For this simplified version, we'll treat all values as usize
                    let value = record.columns[column] as usize;
                    let rid = record.rid as usize;

                    // Update hash index
                    self.hash_indices[column]
                        .entry(value)
                        .or_insert_with(Vec::new)
                        .push((rid, value));

                    // Update B-tree index
                    self.btree_indices[column].insert(value, rid);
                }
            }
        }
    }

    /// Add single entry to index
    pub fn index_entry(&mut self, column: usize, rid: usize, value: usize) {
        if column >= self.num_columns {
            panic!("Invalid column number: {}", column);
        }

        // Update hash index
        self.hash_indices[column]
            .entry(value)
            .or_insert_with(Vec::new)
            .push((rid, value));

        // Update B-tree index
        self.btree_indices[column].insert(value, rid);
    }

    pub fn drop_entry(&mut self, column: usize, rid: usize, value: usize) {
        if column >= self.num_columns {
            panic!("Invalid column number: {}", column);
        }

        // Update hash index
        if let Some(entries) = self.hash_indices[column].get_mut(&value) {
            entries.retain(|(r, _)| *r != rid);
            // Remove the key if no entries left
            if entries.is_empty() {
                self.hash_indices[column].remove(&value);
            }
        }

        // Update B-tree index
        if let Some(&existing_rid) = self.btree_indices[column].get(&value) {
            if existing_rid == rid {
                self.btree_indices[column].remove(&value);
            }
        }
    }

    /// Drop index of specific column
    pub fn drop_index(&mut self, column: usize) {
        if column >= self.num_columns {
            panic!("Invalid column number: {}", column);
        }

        // Clear both indices for the specified column
        self.hash_indices[column].clear();
        self.btree_indices[column].clear();
    }

    pub fn update_index(&mut self, column: usize, rid: usize, old_value: usize, new_value: usize) {
        if column >= self.num_columns {
            panic!("Invalid column number: {}", column);
        }

        // Remove old entry
        self.drop_entry(column, rid, old_value);
        
        // Add new entry
        self.index_entry(column, rid, new_value);
    }
}