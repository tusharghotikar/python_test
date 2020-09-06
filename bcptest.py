import bcp

conn = bcp.Connection(host='192.168.56.1', driver='mssql', username='test_user', password='test')
my_bcp = bcp.BCP(conn)
#file = bcp.DataFile(file_path='home/oracle/Downloads/python_test/file.csv', delimiter=',')
file = bcp.DataFile(delimiter=',')
my_bcp.dump(query='select * from sys.tables', output_file=file)
