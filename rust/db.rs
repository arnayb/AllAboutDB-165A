// mod table;

use table::Table;
use std::collections::HashMap;

pub struct DB {
    tables: HashMap<String, Table>, // HashMap of tables
}

impl DB {
    pub fn new() -> DB {
        DB {
            tables: HashMap::new(), // Initialize the HashMap
        }
    }

    pub fn create_table(&mut self, name: String, num_columns: usize, key: i32) {
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
