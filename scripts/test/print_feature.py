import sys 

from pandas import read_csv

from json import loads, load, dump, dumps

workload = sys.argv[1]

data = read_csv("block.csv")
data.sort_values(by=["block_req_count"])

row = loads(data[data["workload"]==workload][["wss", "read_wss", "write_wss", "block_req_count"]].to_json())

print(dumps(row, indent=2))
print(data[data["workload"]==workload][["wss", "block_req_count"]])