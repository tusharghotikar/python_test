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
from datetime import datetime

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


def exp_data(connection,sname,seq_no,full_exp,split_threshold,directory_name):
 job_name = datetime.now().strftime("%m%d%Y%H%M%S")
 schema_name = sname 

 #print ("Exporting data for ",schema_name.lower())

 cursor1 = connection.cursor()
 #job_id = cursor1.var(in)
 #job_id = cursor1.callfunc("DBMS_DATAPUMP.OPEN",int,["EXPORT","SCHEMA","",job_name,"LATEST"])
 # job_id = cursor1.callfunc("create_job",int)
 #print(" Job id is ",job_id)
 job_id = cursor1.var(int)
 cursor1.callproc('create_job', [job_name,job_id])
 dump_file_name = "exp_"+schema_name.replace("$","")+"_"+job_name+seq_no+"%U.dmp"
 log_file_name = "exp_"+schema_name.replace("$","")+"_"+job_name+seq_no+".log"

 
 #print("job id is ",job_id.getvalue())   
 cursor1.callproc('add_dump_file', [job_id,dump_file_name,str(split_threshold)+"M",directory_name])
 #print("after add file")   

 cursor1.callproc('add_log_file', [job_id,log_file_name,directory_name])
 #print("after log file")   
 
 cursor1.callproc("DBMS_DATAPUMP.METADATA_FILTER",[job_id,"SCHEMA_EXPR","IN ('"+schema_name+"')"])
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
 for x in Data:
     print ("\033[34m",x[0].rjust(4,' '),"|",x[1].ljust(30,' '),"|",str(x[2]).ljust(7,' '),"\033[0m")
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

 Data = get_oracle_list(connection,user_list) 


 print_table(Data,split_partition,username,servername,dbname,silent_mode,split_threshold)

 if silent_mode=="N":
  val = input("Enter schema number (q to quit): ")
 else:
  print("Performing silent export of above mentioned tables")
  time.sleep(2)
  val ="*"
 
 f1 = open('test_json.json','w')
 
 #new_dic['import_conf']['user_list'] = {}
 if str(val) == "*":
  for x in Data:
   # Establish the database connection
   table_name = x[1]

   schema_name,dump_file_name = exp_data(connection,table_name,"0","Y",split_threshold,directory_name)
 
   f1.write(schema_name+","+dump_file_name+"\n")
 
 while str(val).lower()!="q"  and str(val).lower()!="*":
 
  val = int(val)-1
  
  # Establish the database connection
  table_name = Data[int(val)][1]
  file_name = table_name.lower()+".csv"
  schema_name,dump_file_name = exp_data(connection,table_name,"0","Y",split_threshold,directory_name)
 
  f1.write(schema_name+","+dump_file_name+"\n")
  val= input("Enter table number (q to quit): ")
 
 else:
  print ("Exiting...")
  connection.close()
 
 
if __name__ == "__main__":
    main()
