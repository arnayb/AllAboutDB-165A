from .table import Table
import os
import pickle


class Database():

    def __init__(self):
        self.tables = {}
        self.bufferpool = [] 
        self.page_table = {} 

    def open(self, path):
        os.makedirs(path, exist_ok=True)

        # iterating through the tables in path
        for table_name in os.listdir(path):
            table_path = os.path.join(path, table_name)

        
            if os.path.isdir(table_path):
                # loading metadata
                meta_file = os.path.join(table_path, 'metadata.pkl')
                #accessing the column count, primary key index
                with open(meta_file, 'rb') as f:
                    table_metadata = pickle.load(f)
                    col_count = table_metadata['col_count']
                    primary_key_index = table_metadata['primary_key_index']

                table = Table(table_name, col_count, primary_key_index)
                self.tables.append(table) 

                for page_type in ['base', 'tail']:
                    page_dir = os.path.join(table_path, page_type)
                    os.makedirs(page_dir, exist_ok=True)

                    target_pages = table.base_pages if page_type == 'base' else table.tail_pages

                    #iterating through the columns
                    for col_index in range(table.col_count):
                        page_file = f"page_{col_index}.dat"
                        page_path = os.path.join(page_dir, page_file)
                        
                        # checking if the requested page is already in bufferpool
                        if page_file in self.page_table:
                            print(f"{page_type()} Page {page_file} located in bufferpool")
                            target_pages[col_index] = self.page_table[page_file]
                            continue

                         # If the requested record not in bufferpool, load from disk
                        if os.path.exists(page_path):
                            with open(page_path, 'rb') as f:
                                pages = msgpack.unpack(f, raw=False)
                                target_pages[col_index] = pages


    def close(self):
        pass

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
