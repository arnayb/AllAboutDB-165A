from .index import Index
from .page import Page
from time import time
import threading
import copy 
from .config import (
    INDIRECTION_COLUMN
)
import concurrent.futures
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)#max 10 threads
class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

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
        self.PinLock = threading.Lock()
        self.columns = [Page() for _ in range(self.num_columns + 4)]
      
    def has_capacity(self):
        return self.num_records < 512
    def __getstate__(self):
        """ Exclude PinLock from being pickled """
        state = self.__dict__.copy()
        if 'PinLock' in state:
            del state['PinLock']  # Remove the lock before pickling
        return state
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
    def new_base_page(self):
        self.num_base_pages += 1
        self.base_pages.append(LogicalPage(self))

    def new_tail_page(self):
        self.num_tail_pages += 1
        self.tail_pages.append(LogicalPage(self))

    def read_base_page(self, col_idx, base_idx, base_pos):
        with self.base_pages[base_idx].PinLock:
            return self.base_pages[base_idx].columns[col_idx].read(base_pos)

    def read_tail_page(self, col_idx, tail_idx, tail_pos):
        with self.tail_pages[tail_idx].PinLock:
            return self.tail_pages[tail_idx].columns[col_idx].read(tail_pos)

    def write_base_page(self, col_idx, value, base_idx = -1, base_pos = -1):
        if base_pos == -1 and not self.base_pages[base_idx].has_capacity():
            self.new_base_page()
        with self.base_pages[base_idx].PinLock:
            self.base_pages[base_idx].columns[col_idx].write(value, base_pos)
    
    def write_tail_page(self, col_idx, value, tail_idx = -1, tail_pos = -1):
        if self.num_tail_pages == 0 or \
          (tail_pos == -1 and not self.tail_pages[tail_idx].has_capacity()):
            self.new_tail_page()
        with self.tail_pages[tail_idx].PinLock:
            self.tail_pages[tail_idx].columns[col_idx].write(value, tail_pos)

    def get_table_stats(self):#for getting table metadata to save
        state = self.__dict__.copy()

        state.pop("base_pages", None)
        state.pop("tail_pages", None)
        return state
    def restore_from_state(self, state):
        """ Restores a table from the saved state and reinitializes pages """
        self.__dict__.update(state)
        self.base_pages = []
        self.tail_pages = []
    def merge(self, bid):
        print("got here1")
        if bid not in self.page_directory:
            print(f"Error: bid {bid} not in page_directory")
            return
        base_idx, base_pos = self.page_directory[bid]
        print("trying to acquire lock")
        with self.base_pages[base_idx].PinLock:
            rid = self.read_base_page(INDIRECTION_COLUMN, base_idx, base_pos)
        print("got here2")
        tail_idx, tail_pos = self.page_directory[rid]
        initial_idx, initial_pos = tail_idx, tail_pos
        counter = 1
        print("got here")
        while rid % 2 == 1:
            print(f"Loop: rid = {rid}")
            counter += 1
            tail_idx, tail_pos = self.table.page_directory[rid]
            with self.tail_pages[tail_idx].PinLock:
                rid = self.table.read_tail_page(INDIRECTION_COLUMN, tail_idx, tail_pos)
            print(f"counter : {counter}")
        if counter > 1000:
            print("merge is happening")
            col = []
            for i in range(self.num_columns):
                  col.append(self.read_tail_page(i, initial_idx, initial_pos))

            with self.base_pages[base_idx].PinLock:
                for col_idx in range(self.num_columns):
                    self.base_pages[base_idx].columns[col_idx].write(col[col_idx], base_pos)
            self.base_pages[base_idx].columns[INDIRECTION_COLUMN].write(bid, base_pos)
        else:  
            return

