from .table import Table
import os
import pickle


class Database():

    def __init__(self):
        self.tables = {}
        self.bufferpool = [] 
        self.page_table = {} 

    def open(self, path):
        pass
        

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
