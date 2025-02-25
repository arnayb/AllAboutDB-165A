from BTrees.OOBTree import OOBTree
from .config import (
    INDIRECTION_COLUMN,
    RID_COLUMN,
    TIMESTAMP_COLUMN,
    SCHEMA_ENCODING_COLUMN
)

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, 
other columns can be indexed through this object. Indices are usually B-Trees, but other data structures
can be used as well.
"""
class Index:
    def __init__(self, table):
        self.table = table
        self.indices = [None] *  table.num_columns

    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):
        if not (0 <= column < self.table.num_columns):
            raise ValueError(f"Invalid column number: {column}")
        
        if self.indices[column].has_key(value):
            return self.indices[column][value]

        return []
    
    """
    Locate all records within range [begin, end] in specified column
    """
    def locate_range(self, begin, end, column):
        if not (0 <= column < self.table.num_columns):
            raise ValueError(f"Invalid column number: {column}")

        return list(self.indices[column].values(begin, end))
    
    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        self.indices[column_number] = OOBTree()
        for base_page in self.table.base_pages:
          for i in range(base_page.num_records):
              # Need to check if it's deleted - not implemented yet
              modified = (base_page.columns[SCHEMA_ENCODING_COLUMN].read(i) >> column_number) & 1
              if modified:
                  tid = base_page.columns[INDIRECTION_COLUMN].read(i)
                  tail_idx, tail_pos = self.table.page_directory[tid]
                  value = self.table.read_tail_page(column_number, tail_idx, tail_pos)
                  rid = tid
              else:
                  rid = base_page.columns[RID_COLUMN].read(i)
              
              if value in self.indices[column_number]:
                  self.indices[column_number][value].append(rid)
              else:
                  self.indices[column_number][value] = [rid]


    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        pass