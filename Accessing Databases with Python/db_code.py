### Scenario ###
"""
Consider a dataset of employee records that is available with an HR team in a CSV file.
As a Data Engineer, you are required to create the database called STAFF and load the contents
of the CSV file as a table called INSTRUCTORS. The headers of the available data are :

Header	Description
ID	    Employee ID
FNAME	First Name
LNAME	Last Name
CITY	City of residence
CCODE	Country code (2 letters)
"""

# Will create the database on a dummy server using SQLite3

import sqlite3
import pandas as pd

# Create and connect our process to a new database STAFF
conn = sqlite3.connect('STAFF.db')

table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Read the CSV file
# This file doesn't contain headers, but we can use the attribute_list list to assign some

df = pd.read_csv('INSTRUCTOR.csv', names = attribute_list)

"""
The pandas library provides easy loading of its dataframes directly to the database. 
For this, you may use the to_sql() method of the dataframe object.

However, while you load the data for creating the table, you need to be careful if a table with the same name already exists in the database. 
If so, and it isn't required anymore, the tables should be replaced with the one you are loading here. You may also need to append some information to an existing table. 
For this purpose, to_sql() function uses the argument if_exists. The possible usage of if_exists is tabulated below.

Argument usage	Description
if_exists = 'fail'	Default. The command doesn't work if a table with the same name exists in the database.
if_exists = 'replace'	The command replaces the existing table in the database with the same name.
if_exists = 'append'	The command appends the new data to the existing table with the same name.
"""

# index=False makes sure the dataframe indices are included as a column

df.to_sql(table_name, conn, if_exists = 'replace', index=False)
print('Table is ready')

########## QUERYING ##########
# The data is now uploaded to the table in the database, so anyone with access to the database
# can retrieve this data by executing SQL queries.

# Some basic queries:
# SELECT for viewing data
# COUNT to count the number of entries

# Viewing all the data in the table:
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Viewing only FNAME column of data:
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Viewing the total number of entries in the table:
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Appending data to the table
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

# Append the data to the instructor table:
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

# Repeat count query and observe increase of 1
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


######### CLOSE CONNECTION ########
# After all queries are executed, need to close the connection to the database.
conn.close()