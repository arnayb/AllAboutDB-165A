from .index import Index
from .page import Page
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

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
        # + 4 for (as declared by global variables)
          # indirection column
          # rid column
          # timestamp column
          # schema encoding column
        self.base_pages = [[] for _ in range(num_columns + 4)]
        self.tail_pages = [[] for _ in range(num_columns + 4)]
        self.index.create_index(key)
        self.bid_counter = 0
        self.tid_counter = 0

    def __merge(self):
        print("merge is happening")
        pass

