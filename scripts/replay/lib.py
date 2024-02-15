from pandas import read_csv 


def get_num_blocks_full(workload_name: str):
    df = read_csv("files/full_workload_feature.csv")
    row = df[df["workload"]==workload_name]
    return int(row["wss"]//4096)