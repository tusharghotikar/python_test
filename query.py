import cx_Oracle
import numpy as np 
import csv

from simple_term_menu import TerminalMenu

# Establish the database connection
connection = cx_Oracle.connect("hr", "hr", "localhost/orcl")

# Obtain a cursor
cursor = connection.cursor()

# Data for binding
managerId = 145
firstName = "Peter"

# Execute the query
#sql = """SELECT first_name
#         FROM employees
#         WHERE manager_id = :mid AND first_name = :fn"""
#cursor.execute(sql, mid = managerId, fn = firstName)

#f = open("myfile.txt", "w")
# Loop over the result set
"""
rows = cursor.fetchall()

for row in rows:
    print(row)
    f.write(str(row))
    f.write('\n')
f.close()
"""

x = [1,2,3] 
a = np.asarray(x) 
#print (a)

sql = """SELECT rownum,table_name from user_tables"""
cursor.execute(sql)
Data = np.array(list(cursor.fetchall()))
#print (Data)
for x in Data:
    print (x[0]," - ",x[1])

val = input("Enter table number: ")

# Establish the database connection
connection1 = cx_Oracle.connect("hr", "hr", "localhost/orcl")
table_name = Data[int(val)][1]
file_name = table_name.lower()+".csv"
print ("Exporting data for ",table_name.lower())

csv_file = open(file_name, "w")
cursor1 = connection1.cursor()
writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
r = cursor1.execute("SELECT * FROM "+table_name.lower()) 
for row in r:
    writer.writerow(row)

cursor1.close()
connection1.close()
csv_file.close()

