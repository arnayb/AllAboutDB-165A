/// not gonna lie pretty much all of query needs to be edited to fit the new design, it's kind of a mess
/// I tried to just move everything over to rust but it's a bit iffy.

use crate::table::Table;
use crate::page::Page;
use crate::constants::PAGE_SIZE;

pub struct Query<'a> {
    table: &'a mut Table<'a>,
}

impl<'a> Query<'a> {
    pub fn new(table: &'a mut Table<'a>) -> Query<'a> {
        Query {
            table: table,
        }
    }
    ///
    // To do: Complete delete and make sure it is implemented correctly.
    ///
    pub fn delete(&mut self, column: usize, value: usize) {
        let rids = self.table.index.as_ref().unwrap().locate(column, value);
        for rid in rids {
            self.table.base_pages[column][rid] = None;
            self.table.tail_pages[column][rid] = None;
            self.table.index.as_mut().unwrap().drop_entry(column, rid, value);
        }
    }

    pub fn insert(&mut self, rid: usize, values: Vec<usize>) {
        if values.len() != self.table.num_columns {
            panic!("Invalid number of columns");
        }

        let rid = self.table.rid_counter;
        self.table.rid_counter += 1;

        ///
        // To do: check to see if page has capacity before inserting
        ///
        
        // Create a new page and insert data into the tail page
        let page = Page::new();
        self.table.page_directory.insert(rid, page);
        self.table.tail_pages.push(Vec::new()); // Create a new empty version for the record
        let page_index = self.table.tail_pages.len() - 1;

        // Insert the values into the tail page
        for (i, &value) in values.iter().enumerate() {
            self.table.tail_pages[page_index].insert(rid, value); // Assuming insert method handles this
            self.table.base_pages[i] = Some(page_index); // Update base pages with page reference
        }
    }

    pub fn update(&mut self, column: usize, old_value: usize, new_value: usize) {
        let rids = self.table.index.as_ref().unwrap().locate(column, old_value);
        for rid in rids {
            self.table.base_pages[column][rid] = Some(new_value);
            self.table.tail_pages[column][rid] = Some(new_value);
            self.table.index.as_mut().unwrap().update_index(column, rid, old_value, new_value);
        }
    }

    // Select records matching the value in the specified column
    pub fn select(&self, column: usize, value: usize, version: Option<usize>) -> Vec<usize> {
        if version.is_none() {
            let rids = self.table.index.as_ref().unwrap().locate(column, value);
            rids.iter().filter_map(|&rid| self.table.base_pages[column][rid]).collect()
        } else {
            let rids = self.table.index.as_ref().unwrap().locate(column, value);
            rids.iter().filter_map(|&rid| self.table.tail_pages[version.unwrap()][rid]).collect()
        }
    }

    // Select records within a range of values for a specific version
    pub fn select_range(&self, column: usize, begin: usize, end: usize, version: Option<usize>) -> Vec<usize> {
        if version.is_none() {
            let rids = self.table.index.as_ref().unwrap().locate_range(begin, end, column);
            rids.iter().filter_map(|&rid| self.table.base_pages[column][rid]).collect()
        } else {
            let rids = self.table.index.as_ref().unwrap().locate_range(begin, end, column);
            rids.iter().filter_map(|&rid| self.table.tail_pages[version].get(rid).flatten()).collect()
        }
    }

    pub fn increment(&mut self, column: usize, value: usize) {
        let rids = self.table.index.as_ref().unwrap().locate(column, value);
        for rid in rids {
            let old_value = self.table.base_pages[column][rid].unwrap();
            let new_value = old_value + 1;
            self.table.base_pages[column][rid] = Some(new_value);
            self.table.tail_pages[column][rid] = Some(new_value);
            self.table.index.as_mut().unwrap().update_index(column, rid, old_value, new_value);
        }
    }

     // Calculate the sum of values in the specified column within a range
     pub fn sum(&self, begin: usize, end: usize, column: usize) -> usize {
        let rids = self.table.index.as_ref().unwrap().locate_range(begin, end, column);
        rids.iter().filter_map(|&rid| self.table.base_pages[column][rid]).sum()
    }

    // Calculate the sum of values in the specified column for a given version within a range
    pub fn sum_version(&self, begin: i32, end: i32, column: usize, version: usize) -> i32 {
        let rids = self.table.index.as_ref().unwrap().locate_range(begin, end, column);
        rids.iter().filter_map(|&rid| self.table.tail_pages[version].get(rid).flatten()).sum()
    }
}
