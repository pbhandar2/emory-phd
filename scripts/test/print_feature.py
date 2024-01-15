import sys 

from pandas import read_csv

from json import loads, load, dump, dumps

workload = sys.argv[1]

data = read_csv("block.csv")

row = loads(data[data["workload"]==workload][["wss", "read_wss", "write_wss"]].to_json())

print(dumps(row, indent=2))