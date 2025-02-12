from lstore.table import (
    Table, 
    Record, 
    INDIRECTION_COLUMN,
    RID_COLUMN,
    TIMESTAMP_COLUMN,
    SCHEMA_ENCODING_COLUMN
)
from lstore.index import Index
from time import time

BASE_RID = 0
TAIL_RID = 0
RID_INT = 3

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):

        if isinstance(columns[0], (list, tuple)):
            rows = columns
        else:
            rows = [columns] #so can be parsed in forloop if only1
        for row in rows:
            
            rid = BASE_RID + self.table.rid_counter
            self.table.rid_counter += 1

            # print(f"rid is {rid}")
            # Schema encoding (all '0' for new base record)
            self.table.base_pages[SCHEMA_ENCODING_COLUMN] = '0' * self.table.num_columns
            # assume length always good
            for col_index, col_value in enumerate(row):
                self.table.base_pages[col_index].append(col_value)

                if self.table.index.hash_indices[col_index] is not None:
                    self.table.index.index_column(col_index, rid, col_value)

            self.table.base_pages[SCHEMA_ENCODING_COLUMN].append('0' * self.table.num_columns)
            self.table.base_pages[RID_COLUMN].append(rid) #
            self.table.base_pages[TIMESTAMP_COLUMN].append(int(time())) 
            self.table.base_pages[INDIRECTION_COLUMN].append(rid) # point to itself first

            self.table.page_directory[rid] = []
            self.table.page_directory[rid].append(len(self.table.base_pages[0]) - 1)  # Position in Base Page
        # print(f"table now: {self.table.base_pages}")
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
        base_page_pos = self.table.index.locate(search_key_index, search_key)

        # if not been modified at all, return needed columns from base page
        records = []
        for base_pos in base_page_pos:
          if abs(relative_version) > len(self.table.page_directory[base_pos]) - 2:
              key = self.table.base_pages[self.table.key][base_pos]
              col = []
              for i in range(self.table.num_columns):
                  if projected_columns_index[i] != 1:
                      continue
                  col.append(self.table.base_pages[i][base_pos])
              records.append(Record(base_pos, key, col))
              continue
          
          # if some been modified, check tail page
          # assuming pos = tid
          tail_page_pos = self.table.page_directory[base_pos][relative_version - 1]
          key = self.table.base_pages[self.table.key][base_pos]
          col = []
          for i in range(self.table.num_columns):
              if projected_columns_index[i] != 1:
                  continue
              
              # if the specific column has been modified <=> schema encoding = 1
              if self.table.base_pages[SCHEMA_ENCODING_COLUMN][base_pos][i] == 1:
                  col.append(self.table.tail_pages[i][tail_page_pos])
              # the value has not been modified
              else:
                  col.append(self.table.base_pages[i][base_pos])
          records.append(Record(base_pos, key, col))

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

        p_key_col = self.table.base_pages[self.table.key]
        record_index = -1
        for x in range(0, len(p_key_col)):
            if p_key_col[x] == primary_key:
                record_index = x
                break

        if record_index == -1:
            return False  

        #assume columns len == len num_columns
        for col_index in range(self.table.num_columns):
            if columns[col_index]==None:
                self.table.tail_pages[col_index].append(self.table.base_pages[col_index][record_index])
                continue
            self.table.base_pages[SCHEMA_ENCODING_COLUMN][record_index][col_index] = 1
            self.table.tail_pages[col_index].append(columns[col_index])
        rid = self.table.base_pages[RID_COLUMN][record_index]
        self.table.tail_pages[RID_COLUMN].append(rid)
        self.table.page_directory[rid].append(len(self.table.tail_pages[0]) - 1)  
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
        pass

    
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
        pass

    
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
