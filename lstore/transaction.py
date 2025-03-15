from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.rollback_data = [] #storing data for rollback
        self.query_objects = {}  # Cache for Query objects by table

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query_method, table, *args):
        self.queries.append((query_method, table, args))
        # Ensure we have a Query object for this table
        if table not in self.query_objects:
            self.query_objects[table] = Query(table)

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        self.rollback_data.clear()

        for query_method, table, args in self.queries:
            primary_key = args[0]  
            
            # Get the Query object for this table
            query_obj = self.query_objects[table]
            
            # Store the original data for rollback
            old_record = query_obj.select(primary_key, table.key, [1] * table.num_columns)
            if old_record:
                self.rollback_data.append((query_obj, primary_key, old_record))

            # Execute the query method
            result = query_method(*args)
            if not result:
                return self.abort()  # if query fails abort

        return self.commit()
    
    def abort(self):
        #roll-back, restoring old values
        for query_obj, primary_key, old_records in self.rollback_data:
            # Use the first record's columns for the update
            if old_records and len(old_records) > 0:
                old_record = old_records[0]
                query_obj.update(primary_key, *old_record.columns)

        return False  # indicating that transaction failed
        
    #Commiting the transaction, returning true if the transaction succeeds
    def commit(self):
        return True  # indicating success