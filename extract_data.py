import cx_Oracle
import numpy as np 
import csv
import gzip
import shutil

username = "hr"
password = "hr"
servername = "localhost"
dbname = "orcl"

def exp_data(tname,seq_no,full_exp,low_row_id,upp_row_id):
 connection1 = cx_Oracle.connect(username,password, servername+"/"+dbname)
 table_name = tname 
 file_name = table_name.lower()+str(seq_no)+".csv"
 print ("Exporting data for ",table_name.lower())

 csv_file = open(file_name, "w")
 cursor1 = connection1.cursor()
 writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
 if full_exp == 'Y':
  sql_query = "SELECT * FROM "+table_nme.lower()
  r = cursor1.execute(sql_query).fetchall()
 else:
  sql_query = "SELECT * FROM "+table_name.lower()+" where rowid>:lowrowid and rowid<:upprowid"
  r = cursor1.execute(sql_query,[low_row_id,upp_row_id]).fetchall()

 for row in r:
   writer.writerow(row)
 print("Data exported!")
 print("\n")

 csv_file.close()
 cursor1.close()
 connection1.close()


def split_rows(tname):
 print ("Splitting rows for .."+tname)
 username = "hr"
 password = "hr"
 servername = "localhost"
 dbname = "orcl"

 # Establish the database connection
 connection = cx_Oracle.connect(username,password, servername+"/"+dbname)
 sql = """select grp,
 dbms_rowid.rowid_create( 1, data_object_id, lo_fno, lo_block, 0 ) min_rid,
 dbms_rowid.rowid_create( 1, data_object_id, hi_fno, hi_block, 10000 ) max_rid
 from (
 select distinct grp,
 first_value(relative_fno) 
 over (partition by grp order by relative_fno, block_id
 rows between unbounded preceding and unbounded following) lo_fno,
 first_value(block_id ) 
 over (partition by grp order by relative_fno, block_id
 rows between unbounded preceding and unbounded following) lo_block,
 last_value(relative_fno) 
 over (partition by grp order by relative_fno, block_id
 rows between unbounded preceding and unbounded following) hi_fno,
 last_value(block_id+blocks-1) 
 over (partition by grp order by relative_fno, block_id
 rows between unbounded preceding and unbounded following) hi_block,
 sum(blocks) over (partition by grp) sum_blocks
 from (
 select relative_fno,
 block_id,
 blocks,
 trunc( (sum(blocks) over (order by relative_fno, block_id)-0.01) /
 (sum(blocks) over ()/4) ) grp
 from dba_extents
 where segment_name = upper(:table_name)
 and owner = user order by block_id
 )
 ),
(select data_object_id from user_objects where object_name = upper(:table_name) )"""

 cursor.execute(sql,["test_data"])
 Data = np.array(list(cursor.fetchall()))
 seq = 0
 for i in Data:
  print(i)
  seq = seq +1
  exp_data("test_data",seq,"N",i[1],i[2])



# Establish the database connection
connection = cx_Oracle.connect(username,password, servername+"/"+dbname)

# Obtain a cursor
cursor = connection.cursor()

sql = """SELECT rownum,table_name from user_tables"""
cursor.execute(sql)
Data = np.array(list(cursor.fetchall()))

print("\033c")
#print (Data)
print ("-----------------------------------Table Data Export Tool----------------------------")
print ("Schema: ",username.ljust(25," ")," Host:",servername.ljust(25," ")," DB Name:",dbname.ljust(25," "))
print ("-------------------------------------------------------------------------------------")
for x in Data:
    print (x[0].rjust(4,' ')," - ",x[1])
print("\n")

val = input("Enter table number (q to quit): ")

if str(val) == "*":
 for x in Data:
  # Establish the database connection
  connection1 = cx_Oracle.connect(username,password, servername+"/"+dbname)
  table_name = x[1]
  file_name = table_name.lower()+".csv"
  print ("Exporting data for ",table_name.lower())

  csv_file = open(file_name, "w")
  cursor1 = connection1.cursor()
  writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
  r = cursor1.execute("SELECT * FROM "+table_name.lower()).fetchall()
  for row in r:
    writer.writerow(row)
  print("Data exported!")
  print("\n")

  csv_file.close()
  cursor1.close()
  connection1.close()
if str(val) == "p":
 split_rows("test")
 exit() 
while str(val).lower()!="q"  and str(val).lower()!="*":

 val = int(val)-1

 # Establish the database connection
 connection1 = cx_Oracle.connect(username,password, servername+"/"+dbname)
 table_name = Data[int(val)][1]
 file_name = table_name.lower()+".csv"
 print ("Exporting data for ",table_name.lower())

 csv_file = open(file_name, "w")
 cursor1 = connection1.cursor()
 writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
 r = cursor1.execute("SELECT * FROM "+table_name.lower()).fetchall()
 for row in r:
    writer.writerow(row)
 print("Data exported!")
 
 print("\n")
 csv_file.close()
 
 gz_name = file_name+".gz"

 with open(file_name, "rb") as f_in:
    with gzip.open(gz_name, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


 val= input("Enter table number (q to quit): ")

 cursor1.close()
 connection1.close()

 
else:
 print ("Exiting...")

