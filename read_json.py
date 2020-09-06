import json
#conf_details = '{username: "hr",password: "hr", host: "localhost", sid: "orcl"}'
#conf_details = '{"username":"hr","password":"hr","host":"localhost","sid":"orcl"}'
with open("conf_details.txt") as f:
 conf_details_dict = json.load(f)
#conf_details_dict = json.loads(conf_details)
print (conf_details_dict['username'])
table_list = conf_details_dict['export_details']['table_list']

print (conf_details_dict['export_details']['table_list'])
for i in table_list:
 print (i)
