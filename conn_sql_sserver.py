import pyodbc

server = '192.168.56.1'
database = 'AdventureWorks2019' 
username = 'test_user' 
password = 'test' 
conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.6.so.1.1};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

cursor = conn.cursor()
sql = "select * from "+database+".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
cursor.execute(sql)

for row in cursor:
    print(row)
