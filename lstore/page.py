import time

class Page:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.is_dirty = False
        self.dirty_map = set()
        self.pin_count = 0  # Track number of active operations using this page
        self.last_accessed = 0  # Timestamp for LRU tracking
    
    # Add pin/unpin methods
    def pin(self):
        self.pin_count += 1
        self.last_accessed = time.time()
    
    def unpin(self):
        if self.pin_count > 0:
            self.pin_count -= 1
    
    # Modify write method to update last_accessed
    def write(self, value, index = -1):
        self.last_accessed = time.time()
        if index == -1 and not self.has_capacity():
            return False
        if index == -1:
            index = self.num_records
            self.num_records += 1
        
        byte_value = value.to_bytes(8, byteorder='big')
        self.data[index * 8: (index + 1) * 8] = byte_value
        
        self.is_dirty = True
        self.dirty_map.add(index)
        return True
    
    # Modify read method to update last_accessed
    def read(self, index):
        self.last_accessed = time.time()
        return int.from_bytes(self.data[index*8 : (index+1)*8], byteorder='big')
    
    def has_capacity(self):
        # Assuming a page can hold 512 records, like LogicalPage
        return self.num_records < 512