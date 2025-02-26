from .table import Record,thread_pool
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
        # base_idx, base_pos = self.table.page_directory[bid]
        # self.table.write_base_page(INDIRECTION_COLUMN, <logical delete>, base_idx, base_pos)
      
        del self.table.index.indices[self.table.key][primary_key]
        return True
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        bid = self.table.bid_counter
        self.table.bid_counter += 2

        key_col = self.table.key
        if self.table.index.locate(key_col, columns[key_col]):
            return False

        for index, value in enumerate(columns):
            self.table.write_base_page(index, value)
        self.table.index.indices[key_col][columns[key_col]] = [bid]

        self.table.write_base_page(SCHEMA_ENCODING_COLUMN, 0)
        self.table.write_base_page(RID_COLUMN, bid)
        self.table.write_base_page(INDIRECTION_COLUMN, bid) # point to itself first
        self.table.page_directory[bid] = [self.table.num_base_pages - 1, self.table.base_pages[-1].num_records]  # Position in Base Page
        self.table.base_pages[-1].num_records += 1

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
        
        records = []
        for bid in bids:
            base_idx, base_pos = self.table.page_directory[bid]
            key = self.table.read_base_page(self.table.key, base_idx, base_pos)
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
                
        return records


    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # make sure columns len == len num_columns
        if len(columns) != self.table.num_columns:
            return False
        
        record_index = self.table.index.locate(self.table.key, primary_key)
        if not record_index:  # If no record found
            return False

        bid = record_index[0]

        # Check if trying to update to an existing primary key
        if columns[self.table.key] != None and columns[self.table.key] != primary_key:
            # If the new primary key already exists in another record, reject the update
            existing_records = self.table.index.locate(self.table.key, columns[self.table.key])
            if existing_records:
                return False
            del self.table.index.indices[self.table.key][primary_key]
            self.table.index.indices[self.table.key][columns[self.table.key]] = [bid]

        base_idx, base_pos = self.table.page_directory[bid]
        schema_encoding = self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx, base_pos)
        
        if schema_encoding:
            tid = self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos)
            tail_idx, tail_pos = self.table.page_directory[tid]
            for i, value in enumerate(columns):
                if value == None:
                    value = self.table.read_tail_page(i, tail_idx, tail_pos)
                else:
                    schema_encoding = schema_encoding | 1 << i
                    self.table.write_base_page(SCHEMA_ENCODING_COLUMN, schema_encoding, base_idx, base_pos)
                self.table.write_tail_page(i, value)
        else:
            for i, value in enumerate(columns):
                if value == None:
                    value = self.table.read_base_page(i, base_idx, base_pos)
                else:
                    schema_encoding = schema_encoding | 1 << i
                    self.table.write_base_page(SCHEMA_ENCODING_COLUMN, schema_encoding, base_idx, base_pos)
                self.table.write_tail_page(i, value)

        tid = self.table.tid_counter
        self.table.write_tail_page(INDIRECTION_COLUMN, self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos))
        self.table.write_base_page(INDIRECTION_COLUMN, tid, base_idx, base_pos)
        self.table.write_tail_page(RID_COLUMN, tid)
        self.table.page_directory[tid] = [self.table.num_tail_pages - 1, self.table.tail_pages[-1].num_records]
        self.table.tid_counter += 2
        self.table.tail_pages[-1].num_records += 1
        return True

    
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
        for bid in bids:
          bid = bid[0]
          ver = relative_version
          base_idx, base_pos = self.table.page_directory[bid]
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
