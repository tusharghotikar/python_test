import cx_Oracle
import numpy as np 
import csv
import gzip
import shutil
import time, sys
import os

def update_progress(f_name,progress):
    barLength = 20 # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "Done...\r\n"
    block = int(round(barLength*progress))
    text = "\r"+f_name+": [{0}] {1}% {2}".format( "="*block + " "*(barLength-block), round(progress*100,0), status)
    sys.stdout.write(text)
    sys.stdout.flush()

def zip_file(file_name):

 gz_name = file_name+".gz"

 with open(file_name, "rb") as f_in:
    with gzip.open(gz_name, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
 os.remove(file_name)


def exp_data(tname,seq_no,full_exp,low_row_id,upp_row_id):
 table_name = tname 
 file_name = table_name.lower()+str(seq_no)+".csv"
 #print ("Exporting data for ",table_name.lower())

 csv_file = open(file_name, "w")
 cursor1 = connection.cursor()
 writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
 if full_exp == 'Y':
  sql_query = "SELECT * FROM "+table_name.lower()
  r = cursor1.execute(sql_query).fetchall()
 else:
  sql_query = "SELECT * FROM "+table_name.lower()+" where rowid>:lowrowid and rowid<:upprowid"
  r = cursor1.execute(sql_query,[low_row_id,upp_row_id]).fetchall()
 
 #print("total rows "+str(len(r)))
 j = 0
 for row in r:
   j = j+1
   update_progress(file_name,j/len(r))
   writer.writerow(row)
 print(str(len(r))+" rows exported for "+table_name)

 csv_file.close()

 zip_file(file_name)

 cursor1.close()


def split_rows(tname):
 #print ("Splitting rows for .."+tname)

 # Establish the database connection
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

 cursor.execute(sql,[tname])
 Data = np.array(list(cursor.fetchall()))
 seq = 0
 for i in Data:
  #print(i)
  seq = seq +1
  exp_data(tname,seq,"N",i[1],i[2])


# Establish the database connection

username = "hr"
password = "hr"
servername = "localhost"
dbname = "orcl"
connection = cx_Oracle.connect(username,password, servername+"/"+dbname)

# Obtain a cursor
cursor = connection.cursor()

sql = """select rownum,table_name,num_rows,(select ceil(SUM(BYTES)/1024/1024) from dba_extents where segment_name=a.table_name) Size_MB from user_tables a """
cursor.execute(sql)
Data = np.array(list(cursor.fetchall()))

print("\033c")
#print (Data)
print ("-----------------------------------Table Data Export Tool----------------------------")
print ("Schema: ",username.ljust(25," ")," Host:",servername.ljust(25," ")," DB Name:",dbname.ljust(25," "))
print ("-------------------------------------------------------------------------------------")
print ("\033[32m","S.No              Table Name       Rows   Size(MB)","\033[0m")
for x in Data:
    print ("\033[34m",x[0].rjust(4,' '),"  ",x[1].rjust(20,' '),x[2].rjust(10,' '),x[3].rjust(10,' '),"\033[0m")
print("\n")

val = input("Enter table number (q to quit): ")

if str(val) == "*":
 for x in Data:
  # Establish the database connection
  table_name = x[1]
  if int(x[3])>100: 
   split_rows(table_name)
  else:
   exp_data(table_name,"0","Y",0,0)

while str(val).lower()!="q"  and str(val).lower()!="*":

 val = int(val)-1

 # Establish the database connection
 table_name = Data[int(val)][1]
 file_name = table_name.lower()+".csv"
 if int(Data[int(val)][3])>100:
  split_rows(table_name)
 else:
  exp_data(table_name,"0","Y",0,0)

 val= input("Enter table number (q to quit): ")

else:
 print ("Exiting...")
 cursor.close()
 connection.close()
