from lstore.table import Table, Record
from lstore.index import Index

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

        for query, table, args in self.queries:
            primary_key = args[0]  

            # storing the original data for rollback
            old_record = table.select(primary_key, table.key, [1] * table.num_columns)
            if old_record:
                self.rollback_data.append((table, primary_key, old_record))

            result = query(*args)
            if not result:
                return self.abort()  # if query fails abort

        return self.commit()

    
    def abort(self):
        #roll-back, restoring old values
        for table, primary_key, old_data in self.rollback_data:
            table.update(primary_key, *old_data)  

        return False  # indicating that transaction failed
        

    #Commiting the transaction, returning true if the transaction succeeds
    def commit(self):
        return True  # indicating success

