from lstore.table import Table, Record
from lstore.index import Index
import threading



#Global hashmap for locking
lock_table = {}  # { rid: threading.Lock() }
lock_table_lock = threading.Lock()  


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
        MAX_RETRIES = 5
        retries = 0
        acquired_locks = [] #storing acquired locks

        while retries <= MAX_RETRIES:
            self.rollback_data.clear()
            acquired_locks.clear()

            try:
                for query, table, args in self.queries:
                    primary_key = args[0]
                    rid = table.page_directory.get(primary_key)
                    if rid is None:
                        raise Exception()

                    # acquiring the lock
                    with lock_table_lock:
                        if rid not in lock_table:
                            lock_table[rid] = threading.Lock()

                    if not lock_table[rid].acquire(blocking=False):
                        raise Exception()

                    acquired_locks.append(rid)

                    # for saving the rollback data
                    old_record = table.select(primary_key, table.key, [1] * table.num_columns)
                    if old_record:
                        self.rollback_data.append((table, rid, old_record))

                    
                    result = query(*args)
                    if not result:
                        raise Exception()

                return self.commit(acquired_locks)

            except Exception:
                retries += 1
                self.abort(acquired_locks)

        return False
        
  
    
    def abort(self):
        # roll-back + releasing the acquired locks
        for table, rid, old_data in self.rollback_data:
            table.update(rid, *old_data) #restoring original values

        for rid in acquired_locks:
            lock_table[rid].release() #releasing

        return False  # indicating failure

    
    def commit(self):
        # commit + releasing all acquired locks
        for rid in acquired_locks:
            lock_table[rid].release()  

        return True  # indicating success

