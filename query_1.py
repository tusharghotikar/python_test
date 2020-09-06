import cx_Oracle
import csv

# Establish the database connection
connection = cx_Oracle.connect("hr", "hr", "localhost/orcl")

table_name = input("Enter table name to export: ")
file_name = table_name+".csv"

csv_file = open(file_name, "w")
cursor = connection.cursor()
writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
r = cursor.execute("SELECT * FROM "+table_name) 
for row in cursor:
    writer.writerow(row)

cursor.close()
connection.close()
csv_file.close()

