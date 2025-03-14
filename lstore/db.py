from .table import Table, LogicalPage, ReadWriteLockNoWait
import os
import pickle
from .page import Page
import pdb
import threading

db_instance = None

class Database():
    def __init__(self):
        self.tables = {}
        self.bufferpool = {}  # Dictionary mapping (table_name, page_type, page_index, col_index) to Page objects
        self.bufferpool_capacity = 1000  # Maximum number of pages in buffer pool
        self.bufferpool_lru = []  # Keep track of least recently used pages
        self.bufferpool_lock = threading.RLock()  # Thread-safe access to buffer pool
        self.path = ""

        global db_instance
        db_instance = self

    def get_page_from_bufferpool(self, table_name, page_type, page_index, col_index):
        """Retrieve a page from the buffer pool if it exists"""
        key = (table_name, page_type, page_index, col_index)
        with self.bufferpool_lock:
            if key in self.bufferpool:
                # Update LRU - move to end of list (most recently used)
                self.bufferpool_lru.remove(key)
                self.bufferpool_lru.append(key)
                return self.bufferpool[key]
            return None

    def add_page_to_bufferpool(self, table_name, page_type, page_index, col_index, page):
        """Add a page to the buffer pool, evicting LRU page if necessary"""
        key = (table_name, page_type, page_index, col_index)
        with self.bufferpool_lock:
            # If buffer pool is full, evict least recently used page
            if len(self.bufferpool) >= self.bufferpool_capacity and self.bufferpool_lru:
                lru_key = self.bufferpool_lru.pop(0)
                evicted_page = self.bufferpool.pop(lru_key)
                if evicted_page.is_dirty:
                    # Write dirty page to disk
                    self._write_page_to_disk(lru_key[0], lru_key[1], lru_key[2], lru_key[3], evicted_page)
            
            # Add new page to buffer pool
            self.bufferpool[key] = page
            self.bufferpool_lru.append(key)
            return page

    def _write_page_to_disk(self, table_name, page_type, page_index, col_index, page):
        """Write a page to disk"""
        # Check if page is None before attempting to write
        if page is None:
            print(f"Warning: Attempted to write a None page to disk for {table_name}, {page_type}, {page_index}, {col_index}")
            return
            
        table_path = os.path.join(self.path, table_name)
        if not os.path.exists(table_path):
            os.makedirs(table_path)
        
        folder_name = f"{page_type}_{page_index}"
        folder_path = os.path.join(table_path, folder_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        # Write both the data file and the pkl file with metadata
        data_file = os.path.join(folder_path, f"page_{col_index}.dat")
        pkl_file = os.path.join(folder_path, f"page_{col_index}.pkl")
        
        with open(data_file, "wb") as f:
            f.write(page.data)
        with open(pkl_file, "wb") as f:
            pickle.dump(page.num_records, f)
        
        # Reset dirty flag
        page.is_dirty = False

    def open(self, path):
        global db_instance
        db_instance = self

        self.path = path
        if not os.path.exists(self.path):
            return

        for table_name in os.listdir(self.path):
            tablepath = os.path.join(self.path, table_name)
            if not os.path.isdir(tablepath):
                continue
            
            table = self.load_table(table_name) 
            table_history = self.load_page_histories(table_name)
            
            # Initialize base_pages list with the correct size
            table.base_pages = [None] * table.num_base_pages
            
            for index in range(0, table.num_base_pages):
                if index not in table_history["base"]:
                    continue
                    
                numrecords_list = table_history["base"][index]
                base_path = os.path.join(tablepath, f"base_{index}")
                base_page = LogicalPage(table)
                
                # Load column data for this base page
                for col_index in range(min(len(numrecords_list), table.num_columns + 4)): 
                    numrecords = numrecords_list[col_index]
                    if os.path.exists(base_path):
                        page_filepath = os.path.join(base_path, f"page_{col_index}.dat")
                        if os.path.exists(page_filepath):
                            # Actively load the page data from disk instead of just creating placeholder
                            page = self._load_page_if_needed(table_name, "base", index, col_index)
                            if page:
                                base_page.columns[col_index] = page
                                # Add to buffer pool
                                self.add_page_to_bufferpool(table_name, "base", index, col_index, page)
                
                # Set the correct number of records for this page
                base_page.num_records = max(numrecords_list) if numrecords_list else 0
                
                # Ensure key column is loaded to set up lock_map
                key_page = self._load_page_if_needed(table_name, "base", index, table.key)
                if key_page:
                    base_page.columns[table.key] = key_page
                    for x in range(key_page.num_records):
                        key_value = key_page.read(x)
                        if key_value not in table.lock_map:
                            table.lock_map[key_value] = ReadWriteLockNoWait()
                
                # Store the base page at the correct index
                table.base_pages[index] = base_page

            # Similar improvements for tail pages...
            table.tail_pages = [None] * table.num_tail_pages
            
            for index in range(0, table.num_tail_pages):
                if index not in table_history["tail"]:
                    continue
                    
                numrecords_list = table_history["tail"][index]
                tail_path = os.path.join(tablepath, f"tail_{index}")
                tail_page = LogicalPage(table)
                
                for col_index in range(min(len(numrecords_list), table.num_columns + 4)): 
                    numrecords = numrecords_list[col_index]
                    if os.path.exists(tail_path):
                        page_filepath = os.path.join(tail_path, f"page_{col_index}.dat")
                        if os.path.exists(page_filepath):
                            # Actively load the page data
                            page = self._load_page_if_needed(table_name, "tail", index, col_index)
                            if page:
                                tail_page.columns[col_index] = page
                                # Add to buffer pool
                                self.add_page_to_bufferpool(table_name, "tail", index, col_index, page)
                
                tail_page.num_records = max(numrecords_list) if numrecords_list else 0
                table.tail_pages[index] = tail_page
            
            self.tables[table_name] = table

    def _load_page_if_needed(self, table_name, page_type, page_index, col_index):
        """Load a page from disk if not in bufferpool"""
        page = self.get_page_from_bufferpool(table_name, page_type, page_index, col_index)
        if page is not None:
            return page
        
        # Page not in buffer pool, load from disk
        table_path = os.path.join(self.path, table_name)
        folder_path = os.path.join(table_path, f"{page_type}_{page_index}")
        page_filepath = os.path.join(folder_path, f"page_{col_index}.dat")
        
        if os.path.exists(page_filepath):
            with open(page_filepath, "rb") as f:
                page_data = f.read()
            
            pkl_filepath = os.path.join(folder_path, f"page_{col_index}.pkl")
            if os.path.exists(pkl_filepath):
                try:
                    with open(pkl_filepath, "rb") as f:
                        num_records = pickle.load(f)
                except Exception as e:
                    print(f"Error loading page metadata: {e}")
                    num_records = 0
            else:
                # If metadata file is missing, try to determine records count from data size
                num_records = len(page_data) // 8  # Assuming 8-byte records
            
            page = Page()
            page.data = page_data
            page.num_records = num_records
            page.is_dirty = False  # Reset dirty flag for loaded pages
            
            # Add to buffer pool and return
            return self.add_page_to_bufferpool(table_name, page_type, page_index, col_index, page)
        
        return None

    def close(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)
    
        # Flush all pages in the buffer pool, not just dirty ones
        with self.bufferpool_lock:
            for key, page in list(self.bufferpool.items()):
                if page is None:
                    print(f"Warning: Found None page in buffer pool for key {key}")
                    continue
                table_name, page_type, page_index, col_index = key
                # Write all pages to ensure durability
                self._write_page_to_disk(table_name, page_type, page_index, col_index, page)
        
        # Write all logical pages' data regardless of buffer pool status
        for table_name, table in self.tables.items():
            table_path = os.path.join(self.path, table_name)
            if not os.path.exists(table_path):
                os.makedirs(table_path)
                
            # Write base pages
            for base_idx, base_page in enumerate(table.base_pages):
                if base_page is not None:
                    folder_path = os.path.join(table_path, f"base_{base_idx}")
                    if not os.path.exists(folder_path):
                        os.makedirs(folder_path)
                        
                    for col_idx, page in enumerate(base_page.columns):
                        if page is not None:
                            data_file = os.path.join(folder_path, f"page_{col_idx}.dat")
                            pkl_file = os.path.join(folder_path, f"page_{col_idx}.pkl")
                            
                            with open(data_file, "wb") as f:
                                f.write(page.data)
                            with open(pkl_file, "wb") as f:
                                pickle.dump(page.num_records, f)
            
            # Write tail pages
            for tail_idx, tail_page in enumerate(table.tail_pages):
                if tail_page is not None:
                    folder_path = os.path.join(table_path, f"tail_{tail_idx}")
                    if not os.path.exists(folder_path):
                        os.makedirs(folder_path)
                        
                    for col_idx, page in enumerate(tail_page.columns):
                        if page is not None:
                            data_file = os.path.join(folder_path, f"page_{col_idx}.dat")
                            pkl_file = os.path.join(folder_path, f"page_{col_idx}.pkl")
                            
                            with open(data_file, "wb") as f:
                                f.write(page.data)
                            with open(pkl_file, "wb") as f:
                                pickle.dump(page.num_records, f)
            
            # Save table metadata
            self.save_table(table_name)

        global db_instance
        if db_instance == self:
            db_instance = None

    def save_table(self, table_name):
        try:
            self.tables[table_name].lock_map = {}
            table_save = self.tables[table_name].get_table_stats()
            table_directory = os.path.join(self.path, table_name)
            
            # Create directory structure if it doesn't exist
            if not os.path.exists(table_directory):
                os.makedirs(table_directory)
                
            table_filename = os.path.join(table_directory, f"{table_name}.pkl")
            with open(table_filename, "wb") as f:
                pickle.dump(table_save, f)
        except TypeError as e:
            # Catch TypeError if there is a threading lock that cannot be pickled
            print(f"Error caught while saving table {table_name}: {e}")
    def load_table(self, table_name):
        table_filename = os.path.join(self.path, table_name, f"{table_name}.pkl")
        with open(table_filename, "rb") as f:
            state = pickle.load(f)
        
        table = Table(state["name"], state["num_columns"], state["key"])
        table.restore_from_state(state)  # Restore excluding base_pages and tail_pages
        return table
    
    def load_page_histories(self, table_name):
        tablepath = os.path.join(self.path, table_name)
        page_histories = {"base": {}, "tail": {}}

        if not os.path.exists(tablepath):
            return page_histories 

        # Load base pages
        for base_folder in os.listdir(tablepath):
            if base_folder.startswith("base_"):
                base_index = int(base_folder.split("_")[1])
                colpath = os.path.join(tablepath, base_folder)
                page_histories["base"][base_index] = []
                
                # Initialize with zeros for all potential columns
                column_records = [0] * 100  # Large enough to handle any number of columns
                
                for file in os.listdir(colpath):
                    if file.endswith(".pkl"):
                        page_index = int(file.split("_")[1].split(".")[0])
                        with open(os.path.join(colpath, file), "rb") as f:
                            num_records = pickle.load(f)
                        column_records[page_index] = num_records
                
                # Add only the needed columns based on the actual file data
                page_histories["base"][base_index] = column_records

            elif base_folder.startswith("tail_"):
                tail_index = int(base_folder.split("_")[1])
                colpath = os.path.join(tablepath, base_folder)
                
                # Initialize with zeros for all potential columns
                column_records = [0] * 100  # Large enough to handle any number of columns
                
                for file in os.listdir(colpath):
                    if file.endswith(".pkl"):  
                        page_index = int(file.split("_")[1].split(".")[0])
                        with open(os.path.join(colpath, file), "rb") as f:
                            num_records = pickle.load(f)
                        column_records[page_index] = num_records
                        
                page_histories["tail"][tail_index] = column_records

        return page_histories

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):

        if name in self.tables:
            print("Table already exists")
            return self.tables[name]
                
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        # del will trigger KeyError if name does not exist
        del self.tables[name]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]