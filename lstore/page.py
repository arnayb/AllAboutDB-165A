class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        #for tracking the 'dirty' state
        self.is_dirty = False 
        self.dirty_map = set()

    # each page can hold 512 records of 64-bit int (8byte)
    def has_capacity(self):
        return self.num_records < 512

    """
    # param value: int - value to be written
    # Returns True if write is succesful
    # Returns False if page is full
    """
    def write(self, value, index = -1):
        if index == -1 and not self.has_capacity():
            return False
        if index == -1:
            index = self.num_records
            self.num_records += 1
        
        byte_value = value.to_bytes(8, byteorder='big')
        self.data[index * 8: (index + 1) * 8] = byte_value
        
        #once page is written mark as dirty
        self.is_dirty = True
        self.dirty_map.add(index)
        return True

    def read(self, index):
        return int.from_bytes(self.data[index*8 : (index+1)*8], byteorder='big')
