from .table import Record,ReadWriteLockNoWait,thread_pool
from .config import (
    INDIRECTION_COLUMN,
    RID_COLUMN,
    TIMESTAMP_COLUMN,
    SCHEMA_ENCODING_COLUMN,
    MAX_VERSIONS
)
from time import time

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        bid = self.table.index.locate(self.table.key, primary_key)
        if len(bid) == 0:
            return False

        # Need to decide which value to use logical delete
        #base_idx, base_pos = self.table.page_directory[bid]
        #self.table.write_base_page(INDIRECTION_COLUMN, -1, base_idx, base_pos)

        del self.table.index.indices[self.table.key][primary_key]
        return True
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        #print(f"Inserting columns: {columns}")
        bid = self.table.bid_counter
        self.table.bid_counter += 2

        if self.table.should_merge():
            self.table.merge()

        key_col = self.table.key
        if self.table.index.locate(key_col, columns[key_col]):
            return False
        
        self.table.lock_map[columns[key_col]] = ReadWriteLockNoWait()
        
        # Get current base page and record position
        base_idx = self.table.num_base_pages - 1
        base_pos = self.table.base_pages[base_idx].num_records
        
        # Write actual data columns with explicit positions
        for col_idx, value in enumerate(columns):
            # Ensure the page exists for this column
            self.table.write_base_page(col_idx, value, base_idx, base_pos)
        
        # Write metadata columns with explicit positions
        self.table.write_base_page(SCHEMA_ENCODING_COLUMN, 0, base_idx, base_pos)
        self.table.write_base_page(RID_COLUMN, bid, base_idx, base_pos)
        self.table.write_base_page(INDIRECTION_COLUMN, bid, base_idx, base_pos)  # point to itself first
        self.table.write_base_page(TIMESTAMP_COLUMN, 0, base_idx, base_pos)
        
        # Update index and page directory
        self.table.index.indices[key_col][columns[key_col]] = [bid]
        self.table.page_directory[bid] = [base_idx, base_pos]  # Position in Base Page
        
        # Only increment record count once, after all columns are written
        self.table.base_pages[base_idx].num_records += 1

        return True

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        bids = self.table.index.locate(search_key_index, search_key)
        
        if self.table.should_merge():
            self.table.merge()

        if len(bids) == 0:
            return []
        
        records = []
        for bid in bids:
            base_idx, base_pos = self.table.page_directory[bid]
            key = self.table.read_base_page(self.table.key, base_idx, base_pos)
            
            # Make sure this key has a lock
            if key not in self.table.lock_map:
                self.table.lock_map[key] = ReadWriteLockNoWait()
                
            if not self.table.lock_map[key].try_acquire_read():
                return False
            col = []
            rid = self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos)
            
            # backtrack until the rid = bid or reached wanted version
            while rid & 1 and relative_version < 0:
                relative_version += 1
                tail_idx, tail_pos = self.table.page_directory[rid]
                rid = self.table.read_tail_page(INDIRECTION_COLUMN, tail_idx, tail_pos)
                
            if rid & 1:
                tail_idx, tail_pos = self.table.page_directory[rid]
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] != 1:
                        continue
                    col.append(self.table.read_tail_page(i, tail_idx, tail_pos))
            else:
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] != 1:
                        continue
                    col.append(self.table.read_base_page(i, base_idx, base_pos))
            records.append(Record(rid, key, col))
            self.table.lock_map[key].release_read() 
        return records
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        if len(columns) != self.table.num_columns:
            return False
        
        # Quick check if anything needs updating
        if all(col is None for col in columns):
            return True
        
        # Check if record exists
        record_indices = self.table.index.locate(self.table.key, primary_key)
        if len(record_indices) == 0:  
            # Handle insert case for non-existent record if all columns are provided
            if None not in columns:
                return self.insert(*columns)
            return False 

        bid = record_indices[0]
        base_idx, base_pos = self.table.page_directory[bid]
        
        # Prepare primary key update if needed
        new_primary_key = columns[self.table.key]
        primary_key_changed = (new_primary_key is not None and new_primary_key != primary_key)
        
        if primary_key_changed:
            # Check if new primary key already exists
            existing_records = self.table.index.locate(self.table.key, new_primary_key)
            if existing_records:
                return False

        # Create lock if needed
        if primary_key not in self.table.lock_map:
            self.table.lock_map[primary_key] = ReadWriteLockNoWait()
        
        # Try to acquire write lock with minimal blocking time
        if not self.table.lock_map[primary_key].try_acquire_write():
            return False
        
        try:
            # Batch read metadata
            schema_encoding = self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx, base_pos)
            indirection = self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos)
            
            # Optimize column updates tracking
            columns_to_update = []
            updated_values = []
            
            # Only read values for columns that are being updated
            for i, value in enumerate(columns):
                if value is not None:
                    columns_to_update.append(i)
                    updated_values.append(value)
            
            # If no columns need updating, return early
            if not columns_to_update:
                return True
            
            # Batch read current values
            current_values = {}
            if indirection & 1:  # Record has updates
                # Get the most recent tail record
                tail_idx, tail_pos = self.table.page_directory[indirection]
                
                # Read all needed columns in one batch
                for i in columns_to_update:
                    current_values[i] = self.table.read_tail_page(i, tail_idx, tail_pos)
            else:
                # Read from base page
                for i in columns_to_update:
                    current_values[i] = self.table.read_base_page(i, base_idx, base_pos)
            
            # Check if any values actually changed
            actual_updates_needed = False
            for i, value in zip(columns_to_update, updated_values):
                if i in current_values and value != current_values[i]:
                    actual_updates_needed = True
                    break
                    
            # If no actual changes, return early
            if not actual_updates_needed:
                return True
                
            # Calculate new schema encoding
            new_schema = schema_encoding
            for i in columns_to_update:
                new_schema |= (1 << i)
            
            # Pre-allocate tail page if needed
            tail_idx = self.table.num_tail_pages - 1
            need_new_tail_page = (self.table.num_tail_pages == 0 or 
                                not self.table.tail_pages[tail_idx].has_capacity())
            
            if need_new_tail_page:
                self.table.new_tail_page()
                tail_idx = self.table.num_tail_pages - 1
            
            tail_pos = self.table.tail_pages[tail_idx].num_records
            
            # Prepare all values to be written at once
            tid = self.table.tid_counter
            current_time = int(time())
            
            # Use a single batch write for all tail page updates
            self._batch_write_tail_record(
                tid, 
                indirection, 
                current_time, 
                new_schema, 
                columns_to_update, 
                updated_values, 
                current_values, 
                tail_idx, 
                tail_pos, 
                base_idx,
                base_pos
            )
            
            # Update base record - only the necessary fields
            self.table.write_base_page(INDIRECTION_COLUMN, tid, base_idx, base_pos)
            
            # Only update schema if it changed
            if new_schema != schema_encoding:
                self.table.write_base_page(SCHEMA_ENCODING_COLUMN, new_schema, base_idx, base_pos)
            
            # Update page directory
            self.table.page_directory[tid] = [tail_idx, tail_pos]
            self.table.tid_counter += 2
            self.table.tail_pages[tail_idx].num_records += 1
            self.table.updates += 1
            
            # Update index if primary key changed
            if primary_key_changed:
                del self.table.index.indices[self.table.key][primary_key]
                self.table.index.indices[self.table.key][new_primary_key] = [bid]
                
                # Update lock map
                if new_primary_key not in self.table.lock_map:
                    self.table.lock_map[new_primary_key] = ReadWriteLockNoWait()
            
            return True
        finally:
            # Always release the lock
            self.table.lock_map[primary_key].release_write()

    # Add this helper method to the Query class
    def _batch_write_tail_record(self, tid, indirection, timestamp, schema, 
                            columns_to_update, updated_values, current_values, 
                            tail_idx, tail_pos, base_idx, base_pos):
        # Write metadata columns
        self.table.write_tail_page(INDIRECTION_COLUMN, indirection, tail_idx, tail_pos)
        self.table.write_tail_page(RID_COLUMN, tid, tail_idx, tail_pos)
        self.table.write_tail_page(TIMESTAMP_COLUMN, timestamp, tail_idx, tail_pos)
        self.table.write_tail_page(SCHEMA_ENCODING_COLUMN, schema, tail_idx, tail_pos)
        
        # Write updated column values
        for i, value in zip(columns_to_update, updated_values):
            self.table.write_tail_page(i, value, tail_idx, tail_pos)
        
        # Write unchanged column values 
        for i in range(self.table.num_columns):
            if i not in columns_to_update:
                if i in current_values:
                    self.table.write_tail_page(i, current_values[i], tail_idx, tail_pos)
                else:
                    # Need to fetch this value
                    if indirection & 1:
                        prev_tail_idx, prev_tail_pos = self.table.page_directory[indirection]
                        value = self.table.read_tail_page(i, prev_tail_idx, prev_tail_pos)
                    else:
                        # Use the base record info we already have
                        # This assumes base_idx and base_pos are passed in or available in the class
                        # If not, you should pass them to this method
                        value = self.table.read_base_page(i, base_idx, base_pos)
                    self.table.write_tail_page(i, value, tail_idx, tail_pos)
        
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        total = 0
        bids = self.table.index.locate_range(start_range, end_range, self.table.key)
        if len(bids) == 0:
            return False
        
        if self.table.should_merge():
            self.table.merge()
            
        for bid in bids:
          bid = bid[0]
          ver = relative_version
          base_idx, base_pos = self.table.page_directory[bid]
          pkey = self.table.read_base_page(self.table.key, base_idx, base_pos)
          if not self.table.lock_map[pkey].try_acquire_read():
              return False
          rid = self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos)
          # backtrack until the rid = bid or reached wanted version
          while rid & 1 and ver < 0:
              ver += 1
              tail_idx, tail_pos = self.table.page_directory[rid]
              rid = self.table.read_tail_page(INDIRECTION_COLUMN, tail_idx, tail_pos)

          if rid & 1:
              tail_idx, tail_pos = self.table.page_directory[rid]
              total += self.table.read_tail_page(aggregate_column_index, tail_idx, tail_pos)
          else:
              total += self.table.read_base_page(aggregate_column_index, base_idx, base_pos)
          self.table.lock_map[pkey].release_read()
        return total

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False