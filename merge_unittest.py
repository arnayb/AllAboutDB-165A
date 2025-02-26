import unittest
from unittest.mock import patch, MagicMock
import time
import sys
import os

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lstore.table import Table
from lstore.query import Query
from lstore.config import (
    INDIRECTION_COLUMN,
    RID_COLUMN,
    TIMESTAMP_COLUMN,
    SCHEMA_ENCODING_COLUMN,
)

class TestTableMerge(unittest.TestCase):
    def setUp(self):
        # Create a table with 3 columns and primary key at index 0
        self.table = Table("test_table", 3, 0)
        self.query = Query(self.table)
        
    def test_merge_single_update(self):
        """Test merging a single update into base page"""
        # Insert a record
        self.query.insert(10, 20, 30)  # Primary key is 10
        
        # Get the RID of the inserted record
        bid = 0  # First record will have bid=0 based on the initialization
        base_idx, base_pos = self.table.page_directory[bid]
        
        # Check initial state
        self.assertEqual(self.table.read_base_page(0, base_idx, base_pos), 10)
        self.assertEqual(self.table.read_base_page(1, base_idx, base_pos), 20)
        self.assertEqual(self.table.read_base_page(2, base_idx, base_pos), 30)
        self.assertEqual(self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos), bid)
        self.assertEqual(self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx, base_pos), 0)
        
        # Update the record (column 1)
        updated_columns = [None, 25, None]
        self.query.update(10, *updated_columns)
        
        # Verify update was made to tail page
        tid = 1  # First update will have tid=1 based on the initialization
        tail_idx, tail_pos = self.table.page_directory[tid]
        
        # Check that indirection column in base page points to tail page
        self.assertEqual(self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos), tid)
        
        # Check that schema encoding indicates column 1 was updated
        self.assertEqual(self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx, base_pos), 1 << 1)
        
        # Check tail page values
        self.assertEqual(self.table.read_tail_page(0, tail_idx, tail_pos), 10)
        self.assertEqual(self.table.read_tail_page(1, tail_idx, tail_pos), 25)
        self.assertEqual(self.table.read_tail_page(2, tail_idx, tail_pos), 30)
        
        # Now run merge
        self.table.merge()
        
        # Verify that the base page has been updated and metadata reset
        self.assertEqual(self.table.read_base_page(0, base_idx, base_pos), 10)
        self.assertEqual(self.table.read_base_page(1, base_idx, base_pos), 25)  # Updated value
        self.assertEqual(self.table.read_base_page(2, base_idx, base_pos), 30)
        self.assertEqual(self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos), bid)  # Reset to point to itself
        self.assertEqual(self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx, base_pos), 0)  # Reset schema encoding
        
    def test_merge_multiple_updates(self):
        """Test merging multiple updates to the same record"""
        # Insert a record
        self.query.insert(10, 20, 30)
        
        # Get the RID of the inserted record
        bid = 0
        base_idx, base_pos = self.table.page_directory[bid]
        
        # Update column 1
        self.query.update(10, None, 25, None)
        
        # Update column 2
        self.query.update(10, None, None, 35)
        
        # Check current state of base page
        self.assertEqual(self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx, base_pos), (1 << 1) | (1 << 2))
        
        # Run merge
        self.table.merge()
        
        # Verify all values merged correctly
        self.assertEqual(self.table.read_base_page(0, base_idx, base_pos), 10)
        self.assertEqual(self.table.read_base_page(1, base_idx, base_pos), 25)
        self.assertEqual(self.table.read_base_page(2, base_idx, base_pos), 35)
        self.assertEqual(self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos), bid)
        self.assertEqual(self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx, base_pos), 0)
        
    def test_merge_multiple_records(self):
        """Test merging updates for multiple records"""
        # Insert two records
        self.query.insert(10, 20, 30)
        self.query.insert(11, 21, 31)
        
        # Update both records
        self.query.update(10, None, 25, None)
        self.query.update(11, None, None, 36)
        
        # Run merge
        self.table.merge()
        
        # Verify first record
        bid1 = 0
        base_idx1, base_pos1 = self.table.page_directory[bid1]
        self.assertEqual(self.table.read_base_page(0, base_idx1, base_pos1), 10)
        self.assertEqual(self.table.read_base_page(1, base_idx1, base_pos1), 25)
        self.assertEqual(self.table.read_base_page(2, base_idx1, base_pos1), 30)
        self.assertEqual(self.table.read_base_page(INDIRECTION_COLUMN, base_idx1, base_pos1), bid1)
        self.assertEqual(self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx1, base_pos1), 0)
        
        # Verify second record
        bid2 = 2
        base_idx2, base_pos2 = self.table.page_directory[bid2]
        self.assertEqual(self.table.read_base_page(0, base_idx2, base_pos2), 11)
        self.assertEqual(self.table.read_base_page(1, base_idx2, base_pos2), 21)
        self.assertEqual(self.table.read_base_page(2, base_idx2, base_pos2), 36)
        self.assertEqual(self.table.read_base_page(INDIRECTION_COLUMN, base_idx2, base_pos2), bid2)
        self.assertEqual(self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx2, base_pos2), 0)
    
    def test_merge_no_updates(self):
        """Test merging when there are no updates to merge"""
        # Insert a record
        self.query.insert(10, 20, 30)
        
        # Capture the initial state
        bid = 0
        base_idx, base_pos = self.table.page_directory[bid]
        initial_state = {
            "col0": self.table.read_base_page(0, base_idx, base_pos),
            "col1": self.table.read_base_page(1, base_idx, base_pos),
            "col2": self.table.read_base_page(2, base_idx, base_pos),
            "indirection": self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos),
            "schema": self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx, base_pos)
        }
        
        # Run merge (nothing should change)
        self.table.merge()
        
        # Verify state remained the same
        self.assertEqual(self.table.read_base_page(0, base_idx, base_pos), initial_state["col0"])
        self.assertEqual(self.table.read_base_page(1, base_idx, base_pos), initial_state["col1"])
        self.assertEqual(self.table.read_base_page(2, base_idx, base_pos), initial_state["col2"])
        self.assertEqual(self.table.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos), initial_state["indirection"])
        self.assertEqual(self.table.read_base_page(SCHEMA_ENCODING_COLUMN, base_idx, base_pos), initial_state["schema"])

if __name__ == '__main__':
    unittest.main()