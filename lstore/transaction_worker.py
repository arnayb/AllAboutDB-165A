from lstore.table import Table, Record
from lstore.index import Index
import threading
from .config import (ABORTED, FAILED)
import queue

class TransactionWorker(threading.Thread):

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        super().__init__()
        self.stats = []
        if not transactions:
            self.transactions = []
        else:
            self.transactions = transactions
        self.result = 0
    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        # here you need to create a thread and call __run
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()

    """
    Waits for the worker to finish
    """
    def join(self):
        self.thread.join()

    def __run(self):
        for transaction in self.transactions:
              return_val = transaction.run()
              if return_val == ABORTED:
                  self.transactions.append(transaction)
              # self.stats.append(transaction.run())


        # stores the number of transactions that committed
        # self.result = len(list(filter(lambda x: x, self.stats)))

