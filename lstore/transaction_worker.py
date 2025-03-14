from lstore.table import Table, Record
from lstore.index import Index
from threading import Thread

import queue

class TransactionWorker(Thread):

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        super().__init__()
        self.stats = []
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
        self.thread = Thread(target=self.__run)
        self.thread.start()

    """
    Waits for the worker to finish
    """
    def join(self):
        self.thread.join()

    def __run(self):
        for transaction in self.transactions:
              self.stats.append(transaction.run())

        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

