from .table import Record
from .config import (
    INDIRECTION_COLUMN,
    RID_COLUMN,
    TIMESTAMP_COLUMN,
    SCHEMA_ENCODING_COLUMN
)
from .index import Index
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
        rid = self.table.index.locate(primary_key)
        if len(rid) == 0:
            return False
        
        self.table.base_page[INDIRECTION_COLUMN][rid[0]] = -1
        return True
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        bid = self.table.bid_counter
        self.table.bid_counter += 1

        key_col = self.table.key
        if (self.table.index.locate(key_col, columns[key_col])):
            return False

        for index, value in enumerate(columns):
            self.table.base_pages[index].append(value)

        if columns[self.table.key] in self.table.index.indices[self.table.key]:
            self.table.index.indices[self.table.key][columns[self.table.key]].append(bid)
        else:
            self.table.index.indices[self.table.key][columns[self.table.key]] = [bid]

        self.table.base_pages[SCHEMA_ENCODING_COLUMN].append([0] * self.table.num_columns)
        self.table.base_pages[RID_COLUMN].append(bid) 
        self.table.base_pages[INDIRECTION_COLUMN].append(bid) # point to itself first

        self.table.page_directory[bid] = [bid]  # Position in Base Page

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
        # assuming rids = position of that item in the base page
        rids = self.table.index.locate(search_key_index, search_key)

        # if not been modified at all, return needed columns from base page
        records = []
        for rid in rids:
          key = self.table.base_pages[self.table.key][rid]
          col = []
          if abs(relative_version) > len(self.table.page_directory[rid]) - 2:
              for i in range(self.table.num_columns):
                  if projected_columns_index[i] != 1:
                      continue
                  col.append(self.table.base_pages[i][rid])
          else:
              rid = self.table.page_directory[rid][relative_version - 1]
              for i in range(self.table.num_columns):
                  if projected_columns_index[i] != 1:
                      continue
                  col.append(self.table.tail_pages[i][rid])
          records.append(Record(rid, key, col))
              
        return records
    

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        record_index = self.table.index.locate(self.table.key, primary_key)
        if not record_index:  # If no record found
            return False

        bid = record_index[0]
        #assume columns len == len num_columns
        schema_encoding = self.table.base_pages[SCHEMA_ENCODING_COLUMN][bid]
        if 1 in schema_encoding:
            latest_rid = self.table.base_pages[INDIRECTION_COLUMN][bid]
        
        for i, value in enumerate(columns):
            if value == None:
                if schema_encoding[i] & 1:
                    value = self.table.tail_pages[i][latest_rid]
                else:
                    value = self.table.base_pages[i][bid]
                self.table.tail_pages[i].append(value)
                continue
            self.table.base_pages[SCHEMA_ENCODING_COLUMN][bid][i] = 1
            self.table.tail_pages[i].append(value)

        tid = self.table.tid_counter
        self.table.tail_pages[INDIRECTION_COLUMN].append(self.table.base_pages[INDIRECTION_COLUMN][bid])
        self.table.base_pages[INDIRECTION_COLUMN][bid] = tid
        self.table.tail_pages[RID_COLUMN].append(tid)
        self.table.page_directory[bid].append(tid)
        self.table.tid_counter += 1
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
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        
        if len(rids) == 0:
            return False
        
        for rid in rids:
            rid = rid[0]
            if abs(relative_version) > len(self.table.page_directory[rid]) - 2:
                total += self.table.base_pages[aggregate_column_index][rid]
            else:
                tid = self.table.page_directory[rid][relative_version - 1]
                total += self.table.tail_pages[aggregate_column_index][tid]

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
