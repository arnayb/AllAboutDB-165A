from lstore.db import Database
from lstore.query import Query


db = Database()

# Create a table with 5 columns: Student Id (Primary Key), and 4 grades
grades_table = db.create_table('Grades', 5, 0)

# Create a query object for the grades table
query = Query(grades_table)

# Insert 3 records
query.insert([101, 90, 85, 88, 92])  # Insert record 1
query.insert([102, 78, 80, 79, 75])  # Insert record 2
query.insert([103, 95, 96, 90, 94])  # Insert record 3

# Update record with student ID 102 (update their grades)
query.update(102, 102, 88, 85, 80, 78)  # Update student ID 102 grades

# Update record with student ID 103 (update their grades)
query.update(103, 103, 100, 98, 99, 96)  # Update student ID 103 grades

# Output the state of the table and tail pages after operations
print(f"Base Pages: {grades_table.base_pages}")
print(f"Tail Pages: {grades_table.tail_pages}")