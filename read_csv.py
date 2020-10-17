import csv
with open("test_json.json",newline='') as f:
 reader = csv.reader(f)
 data = list(reader)

#print(data)
for i in range(len(data)):
 print(data[i][0]," ",data[i][1])
