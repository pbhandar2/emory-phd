from pandas import read_csv


data = read_csv("block.csv")
data.sort_values(by=["block_req_count"])

print(data[["workload", "wss", "min_offset", "read_wss", "write_wss", "block_req_count"]].to_string())