//mod table;

use table::Table;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::BTreeMap;
use sha2::{Sha256, Digest};

const RID_COLUMN: isize = -2;
const INDIRECTION_COLUMN: isize = -3;
const TIMESTAMP_COLUMN: isize = -4;
const SCHEMA_ENCODING_COLUMN: isize = -5;

pub struct Index {
    table: Table,  // Reference to the parent table
    num_records: Vec<usize>,
    num_buckets: Vec<usize>,
    num_threshold: f64,
    hash_indices: Vec<Option<HashMap<u64, Vec<(usize, i32)>>>>,
    btree_indices: Vec<Option<BTreeMap<i32, usize>>>,
}

impl Index {
    /// Create a new Index instance
    fn new(table: Table) -> Self {
        Self {
            table,
            column_index: HashMap::new(),
        }
    }

    /// Generate a hash for the given value
    fn generate_hash(&self, value: i32, num_buckets: usize) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(value.to_string().as_bytes());
        let result = hasher.finalize();
        
        // Take first 8 bytes and convert to u64
        let hash = u64::from_be_bytes(result[..8].try_into().unwrap());
        hash % (num_buckets as u64)
    }

    /// Find the next prime number after n
    fn find_prime(&self, n: usize) -> usize {
        fn is_prime(num: usize) -> bool {
            if num < 2 {
                return false;
            }
            let sqrt = (num as f64).sqrt() as usize;
            for i in 2..=sqrt {
                if num % i == 0 {
                    return false;
                }
            }
            true
        }

        let mut next_num = n;
        while !is_prime(next_num) {
            next_num += 1;
        }
        next_num
    }

    /// Resize hash index when load factor exceeds threshold
    fn resize_hash(&mut self, column: usize) {
        if let Some(old_index) = &self.hash_indices[column] {
            let old_buckets: HashMap<_, _> = old_index.clone();
            let old_num_buckets = self.num_buckets[column];
            
            // Double number of buckets and find next prime
            self.num_buckets[column] = self.find_prime(old_num_buckets * 2);
            let mut new_index = HashMap::new();
            
            // Rehash all entries
            for (_, bucket) in old_buckets {
                for (rid, value) in bucket {
                    let new_bucket = self.generate_hash(value, self.num_buckets[column]);
                    new_index.entry(new_bucket)
                        .or_insert_with(Vec::new)
                        .push((rid, value));
                }
            }
            
            self.hash_indices[column] = Some(new_index);
        }
    }

    /// Check if resizing is needed and perform resize if necessary
    fn check_and_resize(&mut self, column: usize) {
        if self.hash_indices[column].is_some() {
            let load = self.num_records[column] as f64 / self.num_buckets[column] as f64;
            if load > self.num_threshold {
                self.resize_hash(column);
            }
        }
    }

    /// Locate all records with the given value in the specified column
    pub fn locate(&self, column: usize, value: i32) -> Vec<usize> {
        if column >= self.table.num_columns {
            panic!("Invalid column number: {}", column);
        }

        let mut results = Vec::new();

        // Try hash index first
        if let Some(hash_index) = &self.hash_indices[column] {
            let bucket_num = self.generate_hash(value, self.num_buckets[column]);
            if let Some(bucket) = hash_index.get(&bucket_num) {
                results.extend(
                    bucket
                        .iter()
                        .filter(|(_, val)| *val == value)
                        .map(|(rid, _)| *rid)
                );
            }
        }
        
        // Try B-tree index if hash index didn't find anything
        if results.is_empty() {
            if let Some(btree_index) = &self.btree_indices[column] {
                if let Some(&rid) = btree_index.get(&value) {
                    results.push(rid);
                }
            }
        }

        // Fall back to brute force if needed
        if results.is_empty() {
            results.extend(
                self.table.base_pages[column]
                    .iter()
                    .enumerate()
                    .filter(|(_, &val)| val == Some(value))
                    .map(|(i, _)| i)
            );
        }

        results
    }

    /// Locate all records within range [begin, end] in specified column
    pub fn locate_range(&self, begin: i32, end: i32, column: usize) -> Vec<usize> {
        if column >= self.table.num_columns {
            panic!("Invalid column number: {}", column);
        }

        let (begin, end) = if begin > end {
            (end, begin)
        } else {
            (begin, end)
        };

        let mut results = Vec::new();

        // Try B-tree index first
        if let Some(btree_index) = &self.btree_indices[column] {
            results.extend(
                btree_index
                    .range(begin..=end)
                    .map(|(_, &rid)| rid)
            );
        }

        // Fall back to brute force if B-tree didn't find anything
        if results.is_empty() {
            results.extend(
                self.table.base_pages[column]
                    .iter()
                    .enumerate()
                    .filter(|(_, &val)| {
                        if let Some(v) = val {
                            begin <= v && v <= end
                        } else {
                            false
                        }
                    })
                    .map(|(i, _)| i)
            );
        }

        results
    }

    /// Create index on specific column
    pub fn index_column(&mut self, column: usize) {
        if column >= self.table.num_columns {
            panic!("Invalid column number: {}", column);
        }

        // Initialize hash index
        let mut hash_index = HashMap::new();
        self.num_records[column] = 0;

        // Initialize B-tree index
        let mut btree_index = BTreeMap::new();

        // Populate both indices
        for (i, value) in self.table.base_pages[column].iter().enumerate() {
            if let Some(val) = value {
                let rid = self.table.base_pages[RID_COLUMN as usize][i]
                    .expect("RID column should not contain null values") as usize;

                let bucket_num = self.generate_hash(*val, self.num_buckets[column]);
                hash_index
                    .entry(bucket_num)
                    .or_insert_with(Vec::new)
                    .push((rid, *val));
                
                self.num_records[column] += 1;
                btree_index.insert(*val, rid);
            }
        }

        self.hash_indices[column] = Some(hash_index);
        self.btree_indices[column] = Some(btree_index);
        self.check_and_resize(column);
    }

    /// Add single entry to index
    pub fn index_entry(&mut self, column: usize, rid: usize, value: i32) {
        if column >= self.table.num_columns {
            panic!("Invalid column number: {}", column);
        }

        if self.hash_indices[column].is_some() || self.btree_indices[column].is_some() {
            self.update_index(column, rid, //old, value);
        }

        // Update hash index
        if let Some(hash_index) = &mut self.hash_indices[column] {
            let bucket_num = self.generate_hash(value, self.num_buckets[column]);
            let bucket = hash_index.entry(bucket_num).or_insert_with(Vec::new);
            
            if !bucket.iter().any(|(r, _)| *r == rid) {
                bucket.push((rid, value));
                self.num_records[column] += 1;
                self.check_and_resize(column);
            }
        }

        // Update B-tree index
        if let Some(btree_index) = &mut self.btree_indices[column] {
            btree_index.insert(value, rid);
        }
    }

    pub fn drop_entry(&mut self, column: usize, rid: usize, value: i32) {
        if column >= self.table.num_columns {
            panic!("Invalid column number: {}", column);
        }

        // Update hash index
        if let Some(hash_index) = &mut self.hash_indices[column] {
            let bucket_num = self.generate_hash(value, self.num_buckets[column]);
            if let Some(bucket) = hash_index.get_mut(&bucket_num) {
                bucket.retain(|(r, _)| *r != rid);
                self.num_records[column] -= 1;
            }
        }

        // Update B-tree index
        if let Some(btree_index) = &mut self.btree_indices[column] {
            btree_index.remove(&value);
        }
    }

    /// Drop index of specific column
    pub fn drop_index(&mut self, column: usize) {
        if column >= self.table.num_columns {
            panic!("Invalid column number: {}", column);
        }

        self.hash_indices[column] = None;
        self.btree_indices[column] = None;
        self.num_records[column] = 0;
        self.num_buckets[column] = 101;
    }

    pub fn update_index(&mut self, column, rid, old_value, new_value) {
        if column >= self.table.num_columns {
            panic!("Invalid column number: {}", column);
        }

        // Update hash index
        if let Some(hash_index) = &mut self.hash_indices[column] {
            let old_bucket_num = self.generate_hash(old_value, self.num_buckets[column]);
            let new_bucket_num = self.generate_hash(new_value, self.num_buckets[column]);

            if old_bucket_num != new_bucket_num {
                if let Some(bucket) = hash_index.get_mut(&old_bucket_num) {
                    bucket.retain(|(r, _)| *r != rid);
                }

                hash_index
                    .entry(new_bucket_num)
                    .or_insert_with(Vec::new)
                    .push((rid, new_value));
            } else {
                if let Some(bucket) = hash_index.get_mut(&old_bucket_num) {
                    for (r, v) in bucket.iter_mut() {
                        if *r == rid {
                            *v = new_value;
                        }
                    }
                }
            }
        }

        // Update B-tree index
        if let Some(btree_index) = &mut self.btree_indices[column] {
            btree_index.remove(&old_value);
            btree_index.insert(new_value, rid);
        }
    }   
}