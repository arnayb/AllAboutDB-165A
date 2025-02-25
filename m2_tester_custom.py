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
grades_table = db.tables["Grades"]

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 100
number_of_aggregates = 10
number_of_updates = 10

seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished")

# Check inserted records using select query
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")

db.close()

'''db = Database()
db.open('./ECS165')

# Load the grades table again and print its contents
grades_table = db.get_table('Grades')
query = Query(grades_table)

# Print records after loading the table from the file
print("Loaded records from the table:")
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    print(f"Record for {key}: {record.columns}")

# Close the database after printing
db.close()'''