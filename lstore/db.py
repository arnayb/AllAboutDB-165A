from .table import Table, LogicalPage
import os
import pickle
from .page import Page
import pdb

class Database():

    def __init__(self):
        self.tables = {}
        self.bufferpool = [] 
        self.page_table = {} 
        self.path = ""

    def open(self, path):
        self.path = path
        if not os.path.exists(self.path):
            return

        for table_name in os.listdir(self.path):
            tablepath = os.path.join(self.path, table_name)
            if not os.path.isdir(tablepath):
                continue
            
            table = self.load_table(table_name) 
            table_history = self.load_page_histories(table_name)
            for index in range(0,table.num_base_pages):
                numrecords_list = table_history["base"][index]
                base_path = os.path.join(tablepath, f"base_{index}")
                base_page = LogicalPage(table)
                for col_index in range(table.num_columns + 4): 
                    numrecords = numrecords_list[col_index]
                    if os.path.exists(base_path):
                        page_filepath = os.path.join(base_path, f"page_{col_index}.dat")
                        if os.path.exists(page_filepath):
                            with open(page_filepath, "rb") as f:
                                page_data = f.read()  #read raw bytees
                            page = Page()
                            page.data = page_data 
                            page.num_records = numrecords
                            base_page.columns[col_index]=page

                base_page.num_records = max(numrecords_list)
                table.base_pages.append(base_page)

            for index in range(0,table.num_tail_pages):
                numrecords_list = table_history["tail"][index]
                tail_path = os.path.join(tablepath, f"tail_{index}")
                tail_page = LogicalPage(table)
                for col_index in range(table.num_columns + 4): 
                    numrecords = numrecords_list[col_index]
                    if os.path.exists(tail_path):
                        page_filepath = os.path.join(tail_path, f"page_{col_index}.dat")
                        if os.path.exists(page_filepath):
                            with open(page_filepath, "rb") as f:
                                page_data = f.read()  #read raw bytees
                            page = Page()
                            page.data = page_data 
                            page.num_records = numrecords
                            tail_page.columns[col_index] =page

                tail_page.num_records = max(numrecords_list)
                table.tail_pages.append(tail_page)
            self.tables[table_name] = table

            

    


    def close(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        for table in self.tables:
            tablepath = os.path.join(self.path, self.tables[table].name)
            if not os.path.exists(tablepath):
                os.makedirs(tablepath)
            self.save_table(table)
            for index in range(0,self.tables[table].num_base_pages):
                base_page = self.tables[table].base_pages[index]
                colpath = os.path.join(tablepath, f"base_{index}")
                if not os.path.exists(colpath):
                    os.makedirs(colpath)
                for page_index, page in enumerate(base_page.columns):
                    page_filename = os.path.join(colpath, f"page_{page_index}.dat")
                    pkl_filename = os.path.join(colpath, f"page_{page_index}.pkl")
                    with open(page_filename, "wb") as f:
                        f.write(page.data)  # Write the raw byte data from the page object
                    with open(pkl_filename, "wb") as f:
                        pickle.dump(page.num_records, f)
            for index in range(0,self.tables[table].num_tail_pages):
                tail_page = self.tables[table].tail_pages[index]
                colpath = os.path.join(tablepath, f"tail_{index}")
                if not os.path.exists(colpath):
                    os.makedirs(colpath)
                for page_index, page in enumerate(tail_page.columns):
                    page_filename = os.path.join(colpath, f"page_{page_index}.dat")
                    pkl_filename = os.path.join(colpath, f"page_{page_index}.pkl")
                    with open(page_filename, "wb") as f:
                        f.write(page.data)  # Write the raw byte data from the page object
                    with open(pkl_filename, "wb") as f:
                        pickle.dump(page.num_records, f)

    def save_table(self, table_name): #this saves the nonbase/tailpages
        table = self.tables[table_name].get_table_stats()
        table_filename = os.path.join(self.path, table_name,  f"{table_name}.pkl")
        with open(table_filename, "wb") as f:
            pickle.dump(table, f)
        
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

                for file in os.listdir(colpath):
                    if file.endswith(".pkl"):
                        page_index = int(file.split("_")[1].split(".")[0])
                        with open(os.path.join(colpath, file), "rb") as f:
                            num_records = pickle.load(f)
                        page_histories["base"][base_index].append(num_records)

            elif base_folder.startswith("tail_"):
                tail_index = int(base_folder.split("_")[1])
                colpath = os.path.join(tablepath, base_folder)
                page_histories["tail"][tail_index] = []

                for file in os.listdir(colpath):
                    if file.endswith(".pkl"):  
                        page_index = int(file.split("_")[1].split(".")[0])
                        with open(os.path.join(colpath, file), "rb") as f:
                            num_records = pickle.load(f)
                        page_histories["tail"][tail_index].append(num_records)

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
