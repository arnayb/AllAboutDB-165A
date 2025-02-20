use crate::table::Table;
use std::collections::HashMap;

pub struct DB<'a> {
    tables: HashMap<String, Table<'a>>, // HashMap of tables
}

impl<'a> DB<'a> {
    pub fn new() -> DB<'a> {
        DB {
            tables: HashMap::new(), // Initialize the HashMap
        }
    }

    pub fn create_table(&mut self, name: String, num_columns: usize, key: usize) {
        let table = Table::new(num_columns, name.clone(), key); // Create a new table
        self.tables.insert(name, table); // Insert the table into the HashMap
    }

    pub fn drop_table(&mut self, name: String) {
        self.tables.remove(&name); // Remove the table by name
    }

    pub fn get_table(&self, name: String) -> Option<&Table> {
        self.tables.get(&name) // Get a reference to the table by name
    }
}
