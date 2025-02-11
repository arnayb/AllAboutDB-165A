Python 3.12.5 (v3.12.5:ff3bc82f7c9, Aug  7 2024, 05:32:06) [Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Type "help", "copyright", "credits" or "license()" for more information.
>>> #working test
... from lstore.db import Database
... from lstore.query import Query
... 
... # Create a database and table
... db = Database()
... grades_table = db.create_table('Grades', 5, 0)  # 5 columns, primary key at index 0
... query = Query(grades_table)
... 
... # Insert sample records
... query.insert(101, 90, 85, 88, 92)  # Student 101
... query.insert(102, 78, 80, 79, 75)  # Student 102
... query.insert(103, 95, 96, 90, 94)  # Student 103
... 
... # Update Student 102's grades
... query.update(102, None, 88, 85, 80, 78)
... 
... # Compute sum for column index 1 (first grade)
... sum_result = query.sum(101, 103, 1)
... print(f"Sum of column 1 (grades) from Student 101 to 103: {sum_result}")
... 
... # Compute sumVersion for column index 1, using the previous version (before update)
... sum_version_result = query.sumVersion(101, 103, 1, 1)
... print(f"SumVersion of column 1 from Student 101 to 103 (previous version): {sum_version_result}")
... 
... # Print the final state of the table
... print(f"Base Pages: {grades_table.base_pages}")
... print(f"Tail Pages: {grades_table.tail_pages}")
