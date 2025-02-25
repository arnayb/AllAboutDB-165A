
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    # each page can hold 512 records of 64-bit int (8byte)
    def has_capacity(self):
        return self.num_records < 512

    """
    # param value: int - value to be written
    # Returns True if write is succesful
    # Returns False if page is full
    """
    def write(self, value, index = -1):
        if index == -1:
            index = self.num_records
        if not self.has_capacity():
            return False
        
        byte_value = value.to_bytes(8, byteorder='big')
        self.data[index * 8: (index + 1) * 8] = byte_value
        self.num_records += 1
        return True

    def read(self, index):
        return int.from_bytes(self.data[index*8 : (index+1)*8], byteorder='big')