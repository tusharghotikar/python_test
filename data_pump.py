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


def exp_data(connection,sname,seq_no,full_exp):
 job_name = datetime.now().strftime("%m%d%Y%H%M%S")
 schema_name = sname 
 file_name = schema_name.lower()+str(seq_no)+".csv"
 #print ("Exporting data for ",schema_name.lower())

 cursor1 = connection.cursor()
 #job_id = cursor1.var(in)
 #job_id = cursor1.callfunc("DBMS_DATAPUMP.OPEN",int,["EXPORT","SCHEMA","",job_name,"LATEST"])
 
 #print("total rows "+str(job_id))
 #val = "dbms_datapump.ku$_file_type_dump_file"
 #print("job id "+str(job_id))
 dump_file_name = "exp_"+schema_name+"_"+job_name+seq_no+".dmp"
 log_file_name = "exp_"+schema_name+"_"+job_name+seq_no+".log"
 dir_name = "test_abc"
 #cursor1.callproc("DBMS_DATAPUMP.ADD_FILE",[job_id,job_name+seq_no+".dmp","DATA_PUMP_DIR",val])
 #cursor1.execute("begin dpump_add_file; end;")
 #cursor1.execute("begin dbms_datapump.add_file(:1,:2,:3,dbms_datapump.ku$_file_type_dump_file); end;", [job_id,job_name+seq_no+".dmp","DATA_PUMP_DIR"])
 #cursor1.callproc('dpump_add_file',[180])
 #print (sql_q)
 #cursor1.execute(sql_q)
 #cursor1.callproc("dbms_datapump.add_file",[job_id,job_name+seq_no+".log","DATA_PUMP_DIR","'dbms_datapump.KU$_FILE_TYPE_LOG_FILE'"])
 #cursor1.callproc("DBMS_DATAPUMP.METADATA_FILTER",[job_id,"SCHEMA_EXPR","IN ('"+schema_name+"')"])
 #cursor1.callproc("DBMS_DATAPUMP.START_JOB",[job_id])
 cmd = "nohup expdp hr/hr"+"@orcl"+" schemas ="+schema_name+" directory="+dir_name+" dumpfile="+dump_file_name+" logfile="+log_file_name+".log >expdp.log 2>&1 &"
 #print(cmd)
 os.system(cmd)
 print("Export job started. Please check log file "+dump_file_name+" for more details and progress")
 #subprocess.Popen(cmd)
 j = 0
 
 cursor1.close()


def print_table(Data,split_partition,username,servername,dbname,silent_mode,split_threshold):

 bold_format = '\033[1m'
 end_format = '\033[0m'
 print("\033c")
 print ("-----------------------------------Table Data Export Tool----------------------------")
 print ("Schema: ",bold_format+username.ljust(25," ")+end_format," Host:",bold_format+servername.ljust(20," ")+end_format," DB Name:",bold_format+dbname.ljust(25," ")+end_format)
 print ("Mode: ",bold_format+str("Silent" if silent_mode=="Y" else "Interactive").ljust(25," ")+end_format ," Split after(MB):",bold_format+str(split_threshold).ljust(10," ")+end_format," Split partitions:",bold_format+str(split_partition).ljust(10," ")+end_format)
 print ("-------------------------------------------------------------------------------------")
 print ("\033[32m","S.No Schema Name   Rows","\033[0m")
 for x in Data:
     print ("\033[34m",x[0].rjust(4,' '),"  ",x[1].rjust(20,' '),"\033[0m")
 print ("-------------------------------------------------------------------------------------")

def get_oracle_list(connection,table_list):
 cursor = connection.cursor()

 #print (table_list)

 bindValues = table_list
 bindValues = [val.upper() for val in bindValues]
 bindNames = [":" + str(i + 1) for i in range(len(bindValues))]
 sql = "select rownum,username from dba_users"
 cursor.execute(sql)

 Data = np.array(list(cursor.fetchall()))
 cursor.close()
 return Data


def main():
 conf_details_dict = read_config('conf_details.txt')
 table_list = conf_details_dict['export_details']['table_list']
 
 split_threshold = conf_details_dict['export_details']['split_threshold']
 split_partition = conf_details_dict['export_details']['split_partitions']
 silent_mode = conf_details_dict['export_details']['silent_mode']
 
 
 username = conf_details_dict['username']
 password = conf_details_dict['password']
 dbname = conf_details_dict['sid']
 servername = conf_details_dict['host']
 port_no = conf_details_dict['port']
 
 connection = cx_Oracle.connect(username,password, servername+":"+str(port_no)+"/"+dbname)

 Data = get_oracle_list(connection,table_list) 


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
   if int(x[3])>split_threshold: 
    split_rows(connection,split_partition,table_name)
   else:
    exp_data(connection,table_name,"0","Y")
 
 while str(val).lower()!="q"  and str(val).lower()!="*":
 
  val = int(val)-1
 
  # Establish the database connection
  table_name = Data[int(val)][1]
  file_name = table_name.lower()+".csv"
  exp_data(connection,table_name,"0","Y")
 
  val= input("Enter table number (q to quit): ")
 
 else:
  print ("Exiting...")
  connection.close()

if __name__ == "__main__":
    main()
