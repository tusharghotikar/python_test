import psycopg2 
import numpy as np 
import csv
import gzip
import shutil
import time, sys
import os
import json
import time

def read_config(config_file_name):
 with open(config_file_name) as f:
  conf_details_dict = json.load(f)
  return conf_details_dict

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


def exp_data(connection,tname,seq_no,full_exp,low_row_id,upp_row_id,schema_name):
 table_name = tname 
 file_name = table_name.lower()+str(seq_no)+".csv"
 #print ("Exporting data for ",table_name.lower())

 csv_file = open(file_name, "w")
 cursor1 = connection.cursor()
 writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
 if full_exp == 'Y':
  sql_query = "SELECT * FROM "+schema_name+"."+table_name.lower()
  cursor1.execute(sql_query)
  r = cursor1.fetchall()
 else:
  sql_query = "SELECT * FROM "+schema_name+"."+table_name.lower()+" where rowid>:lowrowid and rowid<:upprowid"
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


def split_rows(connection,split_partition,tname,schema_name):
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
 (sum(blocks) over ()/:thresh) ) grp
 from dba_extents
 where segment_name = upper(:table_name)
 and owner = user order by block_id
 )
 ),
(select data_object_id from user_objects where object_name = upper(:table_name) )"""


 cursor = connection.cursor()

 cursor.execute(sql,[split_partition,tname])
 Data = np.array(list(cursor.fetchall()))
 seq = 0
 for i in Data:
  #print(i)
  seq = seq +1
  exp_data(connection,tname,seq,"N",i[1],i[2])

def print_table(Data,split_partition,username,servername,dbname,silent_mode,split_threshold):

 bold_format = '\033[1m'
 end_format = '\033[0m'
 print("\033c")
 print ("-----------------------------------Table Data Export Tool----------------------------")
 print ("Schema: ",bold_format+username.ljust(25," ")+end_format," Host:",bold_format+servername.ljust(20," ")+end_format," DB Name:",bold_format+dbname.ljust(25," ")+end_format)
 print ("Mode: ",bold_format+str("Silent" if silent_mode=="Y" else "Interactive").ljust(25," ")+end_format ," Split after(MB):",bold_format+str(split_threshold).ljust(10," ")+end_format," Split partitions:",bold_format+str(split_partition).ljust(10," ")+end_format)
 print ("-------------------------------------------------------------------------------------")
 print ("\033[32m","S.No              Table Name       Rows   Size(MB)","\033[0m")
 for x in Data:
     print ("\033[34m",str(x[0]).rjust(4,' '),"  ",str(x[1]).ljust(40,' '),str(x[2]).rjust(10,' '),str(x[3]).rjust(10,' '),"\033[0m")
 print ("-------------------------------------------------------------------------------------")

def get_pg_list(connection,table_list):
 cursor = connection.cursor()

 #print (table_list)

 bindValues = table_list
 bindValues = [val.upper() for val in bindValues]
 bindNames = [":" + str(i + 1) for i in range(len(bindValues))]
 sql = "SELECT row_number() OVER () rownum,relname,n_live_tup,round(pg_relation_size(schemaname||'.'||relname)/1024/1024,0) size, schemaname FROM pg_stat_user_tables"
 # sql = "select rownum,table_name,nvl(num_rows,0) num_rows,nvl((select ceil(SUM(BYTES)/1024/1024) from dba_extents where segment_name=a.table_name),0) Size_MB from user_tables a " + \
 #       "where table_name in (%s)" % (",".join(bindNames))
 #cursor.execute(sql, bindValues)
 cursor.execute(sql)

 Data = np.array(list(cursor.fetchall()))
 cursor.close()
 return Data


def main():
 conf_details_dict = read_config('pg_conf_details.txt')
 table_list = conf_details_dict['export_details']['table_list']
 
 split_threshold = conf_details_dict['export_details']['split_threshold']
 split_partition = conf_details_dict['export_details']['split_partitions']
 silent_mode = conf_details_dict['export_details']['silent_mode']
 
 
 username = conf_details_dict['username']
 password = conf_details_dict['password']
 dbname = conf_details_dict['sid']
 servername = conf_details_dict['host']
 portno = conf_details_dict['port']
 
 connection = psycopg2.connect(user =username, password = password, host = servername, port = portno, database = dbname)


 Data = get_pg_list(connection,table_list) 


 print_table(Data,split_partition,username,servername,dbname,silent_mode,split_threshold)

 if silent_mode=="N":
  val = input("Enter table number (q to quit): ")
 else:
  print("Performing silent export of above mentioned tables")
  time.sleep(2)
  val ="*"
 
 if str(val) == "*":
  for x in Data:
   # Establish the database connection
   table_name = x[1]
   schema_name = x[4]
   if int(x[3])>split_threshold: 
    split_rows(connection,split_partition,table_name,schema_name)
   else:
    exp_data(connection,table_name,"0","Y",0,0,schema_name)
 
 while str(val).lower()!="q"  and str(val).lower()!="*":
 
  val = int(val)-1
 
  # Establish the database connection
  table_name = Data[int(val)][1]
  schema_name = Data[int(val)][4]
  
  file_name = table_name.lower()+".csv"
  if int(Data[int(val)][3])>split_threshold:
   split_rows(connection,split_partition,table_name,schema_name)  
  else:
   exp_data(connection,table_name,"0","Y",0,0,schema_name)
 
  val= input("Enter table number (q to quit): ")
 
 else:
  print ("Exiting...")
  connection.close()

if __name__ == "__main__":
    main()
