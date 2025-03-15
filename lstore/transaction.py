from lstore.table import Table, Record
from lstore.index import Index
from .query import Query
import threading

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.rollback_data = [] #storing data for rollback

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
       

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):

        self.rollback_data.clear()
        
        for i, [query, table, args] in enumerate(self.queries):
            if (query.__name__ == 'delete'):
                primary_key = args[0]  
                # storing the original data for rollback
                bid = table.index.locate(table.key, primary_key)
                self.rollback_data.append((table, primary_key, bid))
            elif query.__name__ == 'update':
                primary_key = args[0]  
                # storing the original data for rollback
                temp_query = Query(table)
                old_record = temp_query.select(primary_key, table.key, [1] * table.num_columns)
                self.rollback_data.append((table, primary_key, old_record))
                

            result = query(*args)
            if not result:
                return self.abort(i)  # if query fails abort

        return self.commit()

    
    def abort(self, num_query):
        #roll-back, restoring old values
        for query, table, args in self.queries[:num_query]:
            if (query.__name__ == 'delete' or query.__name__ == 'update'):
                self.abort_delete_or_update(query.__name__)
            elif (query.__name__ == 'insert'):
                if table.index.locate(table.key, args[table.key]):
                    del table.index.indices[table.key][args[table.key]]
            else:
                continue

        return False  # indicating that transaction failed
    
    def abort_delete_or_update(self, func):
        table, primary_key, old_record = self.rollback_data.pop(0)
        if func == 'delete':
            table.index.indices[table.key][primary_key] = old_record
        else:
            q = Query(table)
            q.update(primary_key, old_record)


    #Commiting the transaction, returning true if the transaction succeeds
    def commit(self):
        return True  # indicating success

