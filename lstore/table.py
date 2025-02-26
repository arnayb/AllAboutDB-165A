from .index import Index
from .page import Page
from time import time


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class PageRange:
    """Organizes multiple LogicalPages into a range."""
    def __init__(self, range_id):
        self.range_id = range_id
        self.pages = []  # Stores LogicalPages

    def add_page(self, logical_page):
        """Adds a LogicalPage to this Page Range."""
        self.pages.append(logical_page)

    def get_pages(self):
        """Returns all LogicalPages in this range."""
        return self.pages
class LogicalPage:
    
    def __init__(self, table):
        self.key = table.key
        self.num_columns = table.num_columns
        self.num_records = 0
        # + 4 for (as declared by global variables)
          # indirection column
          # rid column
          # timestamp column
          # schema encoding column
        self.columns = [Page() for _ in range(self.num_columns + 4)]
      
    def has_capacity(self):
        return self.num_records < 512

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.num_base_pages = 1
        self.num_tail_pages = 0
        self.base_pages = [LogicalPage(self)]
        self.tail_pages = []
        self.index.create_index(key)
        self.bid_counter = 0
        self.tid_counter = 1

       #initialize page_ranges
        # store page range objects / page range creation
        self.page_ranges = []
        self.create_page_range()

    def create_page_range(self):
        #Create pr add to the table
        new_range = PageRange(len(self.page_ranges))
        self.page_ranges.append(new_range)

    def assign_page_to_range(self, page):
        #Assigns a page to the most recent pr
        if not self.page_ranges:
            self.create_page_range()
        self.page_ranges[-1].add_page(page)  # Add page to latest range

    def read_base_page(self, col_idx, base_idx, base_pos):
        return self.base_pages[base_idx].columns[col_idx].read(base_pos)

    def read_tail_page(self, col_idx, tail_idx, tail_pos):
        return self.tail_pages[tail_idx].columns[col_idx].read(tail_pos)

    def write_base_page(self, col_idx, value, base_idx = -1, base_pos = -1):
        if base_pos == -1 and not self.base_pages[base_idx].has_capacity():
            self.num_base_pages += 1
            self.base_pages.append(LogicalPage(self))
        
        self.base_pages[base_idx].columns[col_idx].write(value, base_pos)
    
    def write_tail_page(self, col_idx, value, tail_idx = -1, tail_pos = -1):
        if self.num_tail_pages == 0 or \
          (tail_pos == -1 and not self.tail_pages[tail_idx].has_capacity()):
            self.num_tail_pages += 1
            self.tail_pages.append(LogicalPage(self))
        
        self.tail_pages[tail_idx].columns[col_idx].write(value, tail_pos)

    def __merge(self):
        print("merge is happening")
        pass
