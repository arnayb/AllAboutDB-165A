from .index import Index
from .page import Page
import time
import threading
import copy 
from .config import (
    INDIRECTION_COLUMN,
    RID_COLUMN,
    TIMESTAMP_COLUMN,
    SCHEMA_ENCODING_COLUMN,
)
import concurrent.futures
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)#max 10 threads

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        
class PageRange:
    #Organizes multiple LogicalPages into a range

    def __init__(self, range_id):
        self.range_id = range_id
        self.base_pages = [None] * 16  # Fixed-size array for base pages (16)
        self.tail_pages = []  # Dynamic list for tail pages
        self.num_base_pages = 0  # Tracks how many base pages are assigned

    def add_base_page(self, logical_page):
        #Adds LP to the fixed-size base pages array if  space
        if self.num_base_pages < 16:
            self.base_pages[self.num_base_pages] = logical_page
            self.num_base_pages += 1
            return True
        return False  # Return False if base pages are full

    def add_tail_page(self, logical_page):
        #Adds a LogicalPage to the dynamic tail pages list
        self.tail_pages.append(logical_page)

    def get_base_pages(self):
        #Returns all base pages
        return [page for page in self.base_pages if page is not None]

    def get_tail_pages(self):
        #Returns all tail pages
        return self.tail_pages


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
        self.dirty_base_pages = set()
        self.dirty_tail_pages = set()

        self.updates = 0
        self.merge_in_progress = False
        
    def new_base_page(self):
        self.num_base_pages += 1
        self.base_pages.append(LogicalPage(self))

    def new_tail_page(self):
        self.num_tail_pages += 1
        self.tail_pages.append(LogicalPage(self))

        # initialize page_ranges
        # store page range objects / page range creation
        self.page_ranges = []
        self.create_page_range()

    def create_page_range(self):
        # Create pr add to the table
        new_range = PageRange(len(self.page_ranges))
        self.page_ranges.append(new_range)

    def assign_page_to_range(self, page):
        # Assigns a page to the most recent pr
        if not self.page_ranges:
            self.create_page_range()
        self.page_ranges[-1].add_page(page)  # Add page to latest range

    def read_base_page(self, col_idx, base_idx, base_pos):
        with self.base_pages[base_idx].PinLock:
            return self.base_pages[base_idx].columns[col_idx].read(base_pos)

    def read_tail_page(self, col_idx, tail_idx, tail_pos):
        with self.tail_pages[tail_idx].PinLock:
            return self.tail_pages[tail_idx].columns[col_idx].read(tail_pos)

    def write_base_page(self, col_idx, value, base_idx=-1, base_pos=-1):
        if base_pos == -1 and not self.base_pages[base_idx].has_capacity():
            self.num_base_pages += 1
            self.base_pages.append(LogicalPage(self))

        self.base_pages[base_idx].columns[col_idx].write(value, base_pos)

    def write_tail_page(self, col_idx, value, tail_idx=-1, tail_pos=-1):
        if self.num_tail_pages == 0 or \
                (tail_pos == -1 and not self.tail_pages[tail_idx].has_capacity()):
            self.num_tail_pages += 1
            self.tail_pages.append(LogicalPage(self))

        self.tail_pages[tail_idx].columns[col_idx].write(value, tail_pos)

    '''def __merge(self):
        print("merge is happening")
        pass
      
            self.new_base_page()
        with self.base_pages[base_idx].PinLock:
            self.base_pages[base_idx].columns[col_idx].write(value, base_pos)
            self.dirty_base_pages.add((base_idx, col_idx))'''

    
    def write_tail_page(self, col_idx, value, tail_idx = -1, tail_pos = -1):
        if self.num_tail_pages == 0 or \
          (tail_pos == -1 and not self.tail_pages[tail_idx].has_capacity()):
            self.new_tail_page()
        with self.tail_pages[tail_idx].PinLock:
            self.tail_pages[tail_idx].columns[col_idx].write(value, tail_pos)
            self.dirty_tail_pages.add((tail_idx, col_idx))

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


    def merge(self):
        if hasattr(self, 'merge_in_progress') and self.merge_in_progress:
            return False
    
        self.merge_in_progress = True
        merge_future = thread_pool.submit(self._merge_worker)
        merge_future.add_done_callback(self._merge_completed)

        return True
    
    def _merge_worker(self):
        print("Merge started")
        processed_bids = set()
        merge_count = 0

        copy_base_pages = copy.deepcopy(self.base_pages)
        copy_page_directory = copy.deepcopy(self.page_directory)
        
        base_updates = []  # [(base_idx, base_pos, col_idx, value), ...]
        
        # for each base page in our snapshot
        for base_idx in range(len(copy_base_pages)):
            base_page = copy_base_pages[base_idx]
            if base_page is None:
                continue
                
            # for each record in the base page
            for base_pos in range(base_page.num_records):
                try:
                    # read values from the copy to avoid locking
                    bid = base_page.columns[RID_COLUMN].read(base_pos)
                    indirection = base_page.columns[INDIRECTION_COLUMN].read(base_pos)
                    schema_encoding = base_page.columns[SCHEMA_ENCODING_COLUMN].read(base_pos)
                    
                    if bid in processed_bids or indirection == bid:
                        continue
                    
                    if indirection & 1:  # check if indirection points to a tail record
                        update_count = 0
                        current_rid = indirection
                        tail_values = [None] * self.num_columns
                        latest_timestamp = None
                        
                        while current_rid & 1:
                            try:
                                if current_rid not in copy_page_directory:
                                    break
                                    
                                tail_idx, tail_pos = copy_page_directory[current_rid]
                                
                                if tail_idx >= len(self.tail_pages):
                                    break
                                    
                                # use the thread-safe read method for the real data
                                tail_timestamp = self.read_tail_page(TIMESTAMP_COLUMN, tail_idx, tail_pos)
                                
                                if latest_timestamp is None or tail_timestamp > latest_timestamp:
                                    latest_timestamp = tail_timestamp
                                    
                                for col_idx in range(self.num_columns):
                                    if tail_values[col_idx] is None and (schema_encoding >> col_idx) & 1:
                                        tail_values[col_idx] = self.read_tail_page(col_idx, tail_idx, tail_pos)
                                        
                                current_rid = self.read_tail_page(INDIRECTION_COLUMN, tail_idx, tail_pos)
                                update_count += 1
                                
                            except Exception as e:
                                print(f"Error processing tail record: {e}")
                                break
                        
                        # updates for application
                        if update_count > 0:
                            for col_idx in range(self.num_columns):
                                if tail_values[col_idx] is not None:
                                    base_updates.append((base_idx, base_pos, col_idx, tail_values[col_idx]))
                            
                            # metadata updates
                            base_updates.append((base_idx, base_pos, SCHEMA_ENCODING_COLUMN, 0))
                            base_updates.append((base_idx, base_pos, INDIRECTION_COLUMN, bid))
                            
                            if latest_timestamp is not None:
                                base_updates.append((base_idx, base_pos, TIMESTAMP_COLUMN, latest_timestamp))
                            
                            processed_bids.add(bid)
                            merge_count += 1
                except Exception as e:
                    print(f"Error processing base record: {e}")
        
        # Return the prepared updates to be applied by the main thread
        return base_updates, merge_count
    
    def _merge_completed(self, future):
        try:
            # completed future result
            base_updates, merge_count = future.result()
            
            # apply updates atomically on the main thread
            for base_idx, base_pos, col_idx, value in base_updates:
                self.write_base_page(col_idx, value, base_idx, base_pos)
            
            # reset updates counter
            self.updates = 0
            
            print(f"Background merge completed: {merge_count} records updated")
            
            # rebuild indices if needed
            if merge_count > 0:
                for col_idx in range(self.num_columns):
                    if self.index.indices[col_idx] is not None:
                        self.index.drop_index(col_idx)
                        self.index.create_index(col_idx)
        
        except Exception as e:
            print(f"Error in merge completion: {e}")
        
        finally:
            # reset merge flag
            self.merge_in_progress = False
    
    def should_merge(self):
        if hasattr(self, 'merge_in_progress') and self.merge_in_progress:
            return False

        base_records = sum(page.num_records for page in self.base_pages)

        avg_chain_length = self.updates / base_records if base_records> 0 else 0
        
        ### Threshold for deciding to merge ###
        AVG_CHAIN_LENGTH_THRESHOLD = 2.0  # Average chain length
        
        # decide based on update chains
        return avg_chain_length > AVG_CHAIN_LENGTH_THRESHOLD
