//mod table;
//mod index;

use table::Table;
use index::Index;

const RID_COLUMN: isize = -2;
const INDIRECTION_COLUMN: isize = -3;
const TIMESTAMP_COLUMN: isize = -4;
const SCHEMA_ENCODING_COLUMN: isize = -5;

pub struct Query {
    table: Table,
}

impl Query {
    '''
    // To do: Complete delete and make sure it is implemented correctly.
    '''
    pub fn delete(&mut self, column: usize, value: i32) {
        let rids = self.table.index.locate(column, value);
        for rid in rids {
            self.table.base_pages[column][rid] = None;
            self.table.tail_pages[column][rid] = None;
            self.table.index.delete_entry(column, rid, value);
        }
    }

    pub fn insert(&mut self, values: Vec<i32>) {
        if values.len() != self.table.num_columns {
            panic!("Invalid number of columns");
        }

        let rid = self.table.rid_counter;
        self.table.rid_counter += 1;

        '''
        // To do: check to see if page has capacity before inserting
        '''
        
        // Create a new page and insert data into the tail page
        let page = Page::new();
        self.table.page_directory.insert(rid, page);
        self.table.tail_pages.push(Vec::new()); // Create a new empty version for the record
        let page_index = self.table.tail_pages.len() - 1;

        // Insert the values into the tail page
        for (i, &value) in values.iter().enumerate() {
            self.table.tail_pages[page_index].insert(value); // Assuming insert method handles this
            self.table.base_pages[i] = Some(page_index); // Update base pages with page reference
        }
    }

    pub fn update(&mut self, column: usize, old_value: i32, new_value: i32) {
        let rids = self.table.index.locate(column, old_value);
        for rid in rids {
            self.table.base_pages[column][rid] = Some(new_value);
            self.table.tail_pages[column][rid] = Some(new_value);
            self.table.index.update_entry(column, rid, old_value, new_value);
        }
    }

    // Select records matching the value in the specified column
    pub fn select(&self, column: usize, value: i32, version: Option<usize>) -> Vec<i32> {
        if version.is_none() {
            let rids = self.table.index.locate(column, value);
            rids.iter().filter_map(|&rid| self.table.base_pages[column][rid]).collect()
        } else {
            let rids = self.table.index.locate(column, value);
            rids.iter().filter_map(|&rid| self.table.tail_pages[version.unwrap()][rid]).collect()
        }
    }

    // Select records within a range of values for a specific version
    pub fn select_range(&self, column: usize, begin: i32, end: i32, version: Option<usize>) -> Vec<i32> {
        if version.is_none() {
            let rids = self.table.index.locate_range(begin, end, column);
            rids.iter().filter_map(|&rid| self.table.base_pages[column][rid]).collect()
        } else {
            let rids = self.table.index.locate_range(begin, end, column);
            rids.iter().filter_map(|&rid| self.table.tail_pages[version].get(rid).flatten()).collect()
        }
    }

    pub fn increment(&mut self, column: usize, value: i32) {
        let rids = self.table.index.locate(column, value);
        for rid in rids {
            let old_value = self.table.base_pages[column][rid].unwrap();
            let new_value = old_value + 1;
            self.table.base_pages[column][rid] = Some(new_value);
            self.table.tail_pages[column][rid] = Some(new_value);
            self.table.index.update_entry(column, rid, old_value, new_value);
        }
    }

     // Calculate the sum of values in the specified column within a range
     pub fn sum(&self, begin: i32, end: i32, column: usize) -> i32 {
        let rids = self.table.index.locate_range(begin, end, column);
        rids.iter().filter_map(|&rid| self.table.base_pages[column][rid]).sum()
    }

    // Calculate the sum of values in the specified column for a given version within a range
    pub fn sum_version(&self, begin: i32, end: i32, column: usize, version: usize) -> i32 {
        let rids = self.table.index.locate_range(begin, end, column);
        rids.iter().filter_map(|&rid| self.table.tail_pages[version].get(rid).flatten()).sum()
    }
}