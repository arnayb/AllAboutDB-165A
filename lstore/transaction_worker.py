from lstore.table import Table, Record
from lstore.index import Index
from threading import Thread, Lock
from config import NUM_THREADS

import queue

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        # self.stats = []
        self.threads = []
        self.lock = Lock()

        self.transactions = queue.Queue()
        for transaction in transactions:
            self.transactions.put(transaction)

        self.result = 0
        pass

    
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
        for _ in range(NUM_THREADS):
            thread = Thread(target= self.__run)
            thread.start()
            self.threads.append(thread)
    

    """
    Waits for the worker to finish
    """
    def join(self):
        pass


    def __run(self):
        while True:
            transaction = self.transactions.get()
            with self.lock:
                # self.stats.append(transaction.run()) - it was given in the skeleton code but idk if we need stats
                self.result += transaction.run()
                
        # stores the number of transactions that committed
        # self.result = len(list(filter(lambda x: x, self.stats)))

