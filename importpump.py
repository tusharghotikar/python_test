import cx_Oracle
import numpy as np 
import csv
import gzip
import shutil
import time, sys
import os
import json
import time
import subprocess
import csv
from datetime import datetime

def read_config(config_file_name):
 with open(config_file_name) as f:
  conf_details_dict = json.load(f)

  return conf_details_dict

def get_list_data():
 with open("test_json.json",newline='') as f:
  reader = csv.reader(f)
  data = list(reader)

 #print(data)
 for i in range(len(data)):
  print(data[i][0]," ",data[i][1])

  return data

def exp_data(connection,sname,dmp_file_name,seq_no,full_exp,split_threshold,directory_name):
 job_name = datetime.now().strftime("%m%d%Y%H%M%S")
 schema_name = sname 

 #print ("Exporting data for ",schema_name.lower())

 cursor1 = connection.cursor()
 #job_id = cursor1.var(in)
 #job_id = cursor1.callfunc("DBMS_DATAPUMP.OPEN",int,["EXPORT","SCHEMA","",job_name,"LATEST"])
 # job_id = cursor1.callfunc("create_job",int)
 #print(" Job id is ",job_id)
 job_id = cursor1.var(int)
 cursor1.callproc('create_import_job', [job_name,job_id])
 #dump_file_name = "exp_"+schema_name.replace("$","")+"_"+job_name+seq_no+"%U.dmp"
 #log_file_name = "exp_"+schema_name.replace("$","")+"_"+job_name+seq_no+".log"

 dump_file_name = dmp_file_name 
 log_file_name = "imp_"+schema_name.replace("$","")+"_"+job_name+seq_no+".log"
 
 print("job id is ",job_id.getvalue())   
 cursor1.callproc('add_dump_file', [job_id,dump_file_name,str(split_threshold)+"M",directory_name])
 print("after add file-",dump_file_name,"-",str(split_threshold)+"M","-",directory_name)   

 cursor1.callproc('add_log_file', [job_id,log_file_name,directory_name])
 print("after log file-",log_file_name,"-",directory_name)   
 
 #cursor1.callproc("DBMS_DATAPUMP.METADATA_FILTER",[job_id,"SCHEMA_EXPR","IN ('"+schema_name+"')"])
 cursor1.callproc("DBMS_DATAPUMP.START_JOB",[job_id])
 print("Export job "+str(job_id.getvalue())+" started. Please check log file "+dump_file_name+" for more details and progress")
 j = 0
 
 cursor1.close()
 return schema_name,dump_file_name

def print_table(Data,split_partition,username,servername,dbname,silent_mode,split_threshold):

 bold_format = '\033[1m'
 end_format = '\033[0m'
 print("\033c")
 print ("-----------------------------------Table Data Export Tool----------------------------")
 print ("Schema: ",bold_format+username.ljust(25," ")+end_format," Host:",bold_format+servername.ljust(20," ")+end_format," DB Name:",bold_format+dbname.ljust(25," ")+end_format)
 print ("Mode: ",bold_format+str("Silent" if silent_mode=="Y" else "Interactive").ljust(25," ")+end_format ," Split after(MB):",bold_format+str(split_threshold).ljust(10," ")+end_format," Split partitions:",bold_format+str(split_partition).ljust(10," ")+end_format)
 print ("-------------------------------------------------------------------------------------")
 print ("\033[32m",str("S.No").rjust(4,'*'),"|",str("Schema Name").ljust(30,' '),"|",str("Size(M)").ljust(7,'*'),"\033[0m")
 print ("-------------------------------------------------------------------------------------")
 y=1
 for x in range(len(Data)):
     print ("\033[34m",str(y).rjust(4,' '),"|",Data[x][0].ljust(30,' '),"|",str(Data[x][1]).ljust(7,' '),"\033[0m")
     y = y+1
 print ("-------------------------------------------------------------------------------------")


def get_oracle_list(connection,table_list):
 cursor = connection.cursor()

 #print (table_list)

 bindValues = table_list
 bindValues = [val.upper() for val in bindValues]
 bindNames = [":" + str(i + 1) for i in range(len(bindValues))]
 sql = "select rownum,username, nvl((select ceil(SUM(BYTES)/1024/1024) from dba_extents where owner=a.username),0) siz from dba_users a " + \
 "where username in (%s)" % (",".join(bindNames))

 cursor.execute(sql, bindValues)

 Data = np.array(list(cursor.fetchall()))
 cursor.close()
 return Data


def main():
 conf_details_dict = read_config('conf_user_details.txt')
 output_conf = conf_details_dict
 user_list = conf_details_dict['export_details']['user_list']
 
 split_threshold = conf_details_dict['export_details']['split_threshold']
 split_partition = conf_details_dict['export_details']['split_partitions']
 silent_mode = conf_details_dict['export_details']['silent_mode']
 directory_name = conf_details_dict['export_details']['directory']
 
 
 username = conf_details_dict['username']
 password = conf_details_dict['password']
 dbname = conf_details_dict['sid']
 servername = conf_details_dict['host']
 port_no = conf_details_dict['port']
 
 connection = cx_Oracle.connect(username,password, servername+":"+str(port_no)+"/"+dbname)

# Data = get_oracle_list(connection,user_list) 
 Data1 = get_list_data()


 print_table(Data1,split_partition,username,servername,dbname,silent_mode,split_threshold)

 if silent_mode=="N":
  val = input("Enter schema number (q to quit): ")
 else:
  print("Performing silent export of above mentioned tables")
  time.sleep(2)
  val ="*"
 
 Data = np.array(Data1)
 if str(val) == "*":
  for x in Data:
   # Establish the database connection
   schema_name = x[0]
   dump_file_name = x[1]
   print(schema_name," - ",dump_file_name)
   exp_data(connection,schema_name,dump_file_name,"0","Y",split_threshold,directory_name)
 
 while str(val).lower()!="q"  and str(val).lower()!="*":
 
  val = int(val)-1
  
  # Establish the database connection
  schema_name = Data[int(val)][0]
  dump_file_name = Data[int(val)][1]
  exp_data(connection,schema_name,dump_file_name,"0","Y",split_threshold,directory_name)
  val= input("Enter table number (q to quit): ")
 
 else:
  print ("Exiting...")
  connection.close()
 
 
if __name__ == "__main__":
    main()
