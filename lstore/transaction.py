from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
       acquired_locks = []  #tracking the acquired locks for release
        rollback_data = []  # tracking for rollback

        try:
            for query, table, args in self.queries:
                record_id = args[0]  

                # acquiring the 2PL lock
                if not lock_manager.acquire(record_id, "WRITE"):
                    raise Exception("Lock acquisition failed")  # failed to acquire a lock, triggeing rollback

                acquired_locks.append(record_id)  

                # storing the original data for rollback
                old_record = table.select(record_id, table.key, [1] * table.num_columns)
                if old_record:
                    rollback_data.append((table, record_id, old_record))

                result = query(*args)
                if not result:
                    raise Exception("Query execution failed")  # rollback

            return self.commit(acquired_locks)  # Commit if all queries succeed

        except Exception:
            return self.abort(acquired_locks, rollback_data)  

    
    def abort(self):
        # roll-back + releasing the acquired locks
        for table, record_id, old_data in rollback_data:
            table.update(record_id, *old_data)  # restoring the original values

        for record_id in acquired_locks:
            lock_manager.release(record_id)  # releasing

        return False  # indicating failure

    
    def commit(self):
        # commit + releasing all acquired locks
        for record_id in acquired_locks:
            lock_manager.release(record_id)  

        return True  # indicating success

