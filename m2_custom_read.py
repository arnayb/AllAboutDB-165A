from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
#grades_table = db.create_table('Grades', 5, 0)
grades_table = db.tables["Grades"]

# create a query class for the grades table
query = Query(grades_table)

print(f"base: {grades_table.num_base_pages}")
print(f"tail: {grades_table.num_tail_pages}")


"""for x in range(0, grades_table.num_tail_pages):
    tail_page = grades_table.tail_pages[x]
    buffer2 = []
    for page in tail_page.columns:
        buffer = []
        for index in range(0, page.num_records):
            record = page.read(index)
            buffer.append(record)
        print(buffer)
        buffer2.append(buffer)
    print(buffer2)
    for i in range(0, len(buffer2[0])):
        toprint = []
        for j in range(0, len(buffer2)):
            toprint.append(buffer2[j][i])
        print(f"row {i}: {toprint}")"""
