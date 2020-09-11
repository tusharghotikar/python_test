import cx_Oracle
import json
import csv

def read_config(config_file_name):
 with open(config_file_name) as f:
  conf_details_dict = json.load(f)
  return conf_details_dict


def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)

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

 cursor=connection.cursor()
 idVal = 1
 textData = "The quick brown fox jumps over the lazy dog"
 bytesData = b"Some binary data"
 cursor.execute("insert into lob_tbl (id, c, b) values (:1, :2, :3)",
        [idVal, textData, bytesData])

 connection.outputtypehandler = OutputTypeHandler
 
 table_name = "lob_tbl"
 seq_no = 1

 file_name = table_name.lower()+str(seq_no)+".csv"
 #print ("Exporting data for ",table_name.lower())

 csv_file = open(file_name, "w")
 cursor1 = connection.cursor()
 writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)

 cursor.execute("select c, b from lob_tbl where id = :1", ['2'])
 r = cursor.fetchone()
 clobData = r[0]
 blobData = r[1]
 print("CLOB length:", len(clobData))
 print("CLOB data:", clobData)
 print("BLOB length:", len(blobData))
 print("BLOB data:", blobData)
 
 cursor.close()
 connection.close()
if __name__ == "__main__":
    main()
