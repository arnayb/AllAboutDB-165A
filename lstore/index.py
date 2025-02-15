from BTrees.OOBTree import OOBTree
import hashlib
from collections import defaultdict
from typing import List

RID_COLUMN = -2

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, 
other columns can be indexed through this object. Indices are usually B-Trees, but other data structures
can be used as well.
"""
class Index:
    def __init__(self, table):
        self.table = table

        self.num_records = [0] * table.num_columns
        self.num_buckets = [101] * table.num_columns
        self.num_threshold = 0.7

        self.hash_indices = [None] * table.num_columns
        self.btree_indices = [None] * table.num_columns

    """
    # Generates a hash for the given value
    """
    def _generate_hash(self, value: int, num_buckets: int) -> int:
        if value is None:
            return "null"
        
        # Use SHA-256 to generate hash between 0 and num_buckets
        hash = hashlib.sha256(str(value).encode()).hexdigest()
        return int(hash[:8], 16) % num_buckets
    
    """
    Find the next prime number after n
    """
    def _find_prime(self, n: int) -> int:
        def is_prime(num):
            if num < 2:
                return False
            for i in range(2, int(num ** 0.5) + 1):
                if num % i == 0:
                    return False
            return True
        
        next_num = n
        while not is_prime(next_num):
            next_num += 1
        return next_num

    """
    Resize hash index when load factor exceeds threshold
    """    
    def _resize_hash(self, column: int) -> None:
        old_buckets = dict(self.hash_indices[column])
        old_num_buckets = self.num_buckets[column]
        
        # Double number of buckets
        self.num_buckets[column] = self._find_prime(old_num_buckets * 2)
        self.hash_indices[column] = dict(list)
        
        # Rehash all entries
        for bucket in old_buckets.values():
            for rid, value in bucket:
                new_bucket = self._hash_value(value, self.num_buckets[column])
                self.hash_indices[column][new_bucket].append((rid, value))
                
    """
    Check if resizing is needed and perform resize if necessary
    """
    def _check_and_resize(self, column: int) -> None:
        if self.hash_indices[column] is None:
            return
        
        # Calculate load factor and resize if necessary
        load = self.num_records[column] / self.num_buckets[column]
        if load > self.threshold:
            self._resize_hash(column)

    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column: int, value: int) -> List[int]:
        if not (0 <= column < self.table.num_columns):
            raise ValueError(f"Invalid column number: {column}")
            
        # try hash index first
        if self.hash_indices[column] is not None:
            bucket_num = self._hash_value(value, self.num_buckets[column])
            if bucket_num in self.hash_indices[column]:
                bucket = self.hash_indices[column][bucket_num]
                return [rid for rid, val in bucket if val == value]
            
        # try B-tree index
        if self.btree_indices[column] is not None:
            try:
                return [self.btree_indices[column][value]]
            except KeyError:
                pass
                
        # brute force
        return [i for i, val in enumerate(self.table.base_pages[column]) if val == value]
    
    """
    Locate all records within range [begin, end] in specified column
    """
    def locate_range(self, begin: int, end: int, column: int) -> List[int]:
        if not (0 <= column < self.table.num_columns):
            raise ValueError(f"Invalid column number: {column}")
            
        if begin > end:
            begin, end = end, begin

        results = []
            
        # try B-tree
        if self.btree_indices[column] is not None:
            btree = self.btree_indices[column]
            try:
                for value in btree.keys(min=begin, max=end):
                    results.append[btree[value]]
            except KeyError:
                pass
            
        # try brute force
        return [rid for rid, value in enumerate(self.table.base_pages[column]) if begin <= value <= end]
    

    """
    # optional: Create index on specific column
    """
    def index_column(self, column: int) -> None:
        if not (0 <= column < self.table.num_columns):
            raise ValueError(f"Invalid column number: {column}")
            
        # Create hash index with bucketing
        self.hash_indices[column] = defaultdict(list)
        self.num_records[column] = 0
        
        # Create B-tree index
        self.btree_indices[column] = OOBTree()

        for i, value in enumerate(self.table.base_pages[column]):
            if value is not None:
                rid = self.table.base_pages[RID_COLUMN][i]

                bucket_num = self._hash_value(value, self.num_buckets[column])
                if bucket_num not in self.hash_indices[column]:
                    self.hash_indices[column][bucket_num] = []
                self.hash_indices[column][bucket_num].append((rid, value))
                self.num_records[column] += 1
                self._check_and_resize(column)

                self.btree_indices[column][value] = rid


    """
    # add single entry to index
    """
    def index_entry(self, column: int, rid: int, value: int) -> None:
        if not (0 <= column < self.table.num_columns):
            raise ValueError(f"Invalid column number: {column}")
            
        if self.hash_indices[column] is not None:
            bucket_num = self._hash_value(value, self.num_buckets[column])
            if bucket_num not in self.hash_indices[column]: 
                self.hash_indices[column][bucket_num] = []

            if not any(r == rid for r, _ in self.hash_indices[column[bucket_num]]):
                self.hash_indices[column][bucket_num].append((rid, value))
                self.num_records[column] += 1
                self._check_and_resize(column)
            
        if self.btree_indices[column] is not None:
            self.btree_indices[column][value] = rid

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column: int) -> None:
        if not (0 <= column < self.table.num_columns):
            raise ValueError(f"Invalid column number: {column}")
            
        self.hash_indices[column] = None
        self.btree_indices[column] = None
        self.num_records[column] = 0
        self.num_buckets[column] = 101

   ### not sure if drop index is supposed to drop whole column or simple one index entry