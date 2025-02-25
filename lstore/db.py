from .table import Table
import os
import pickle
from .page import Page

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

            for col_index in range(table.num_columns + 4):  
                colpath = os.path.join(tablepath, f"col_{col_index}")

                if os.path.exists(colpath):
                    for page_filename in os.listdir(colpath):
                        if page_filename.endswith(".dat"):
                            page_filepath = os.path.join(colpath, page_filename)
                            with open(page_filepath, "rb") as f:
                                page_data = f.read()  #read raw bytees

                            page = Page()
                            page.data = page_data 
                            table.base_pages[col_index]=(page)
            self.tables[table_name] = table

    


    def close(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        
        for table in self.tables:
            tablepath = os.path.join(self.path, self.tables[table].name)
            if not os.path.exists(tablepath):
                os.makedirs(tablepath)
            

            for index, page in enumerate(self.tables[table].base_pages):
                self.save_table(table)
                colpath = os.path.join(tablepath, f"col_{index}")
                if not os.path.exists(colpath):
                    os.makedirs(colpath)
                
                page_filename = os.path.join(colpath, f"page_{index + 1}.dat")
                with open(page_filename, "wb") as f:
                    f.write(page.data)  # Write the raw byte data from the page object

    def save_table(self, table_name): #this saves the nonbase/tailpages
        table = self.tables[table_name]
        table_filename = os.path.join(self.path, table_name,  f"{table_name}.pkl")
        with open(table_filename, "wb") as f:
            pickle.dump(table, f)
        
    def load_table(self, table_name):
        table_filename = os.path.join(self.path, table_name, f"{table_name}.pkl")

        if not os.path.exists(table_filename):
            print(f"Table {table_name} not found at {table_filename}.")
            return None

        with open(table_filename, "rb") as f:
            table = pickle.load(f)
        
        table.base_pages = [Page() for _ in range(table.num_columns + 4)]
        table.tail_pages = [Page() for _ in range(table.num_columns + 4)]
        print(f"Table {table_name} loaded from {table_filename}")
        return table

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):

        if name in self.tables:
            raise Exception("Table already exists")
                
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
