from pathlib import Path 
from argparse import ArgumentParser
from collections import defaultdict

from pandas import DataFrame, read_csv 

from keyuri.config.BaseConfig import BaseConfig

import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 22})

OUTPUT_DIR = Path("/research2/mtc/cp_traces/pranav-phd/replay_output/set-0")


class OutputFile:
    def __init__(self, output_file_path):
        self._output_file_path = output_file_path 
    
    def get_stat_dict(self):
        cur_dict = {}
        with self._output_file_path.open("r") as f:
            line = f.readline().rstrip()
            while line:
                split_line = line.split("=")
                metric = split_line[0]
                metric_val = float(split_line[1])
                cur_dict[metric] = metric_val
                line = f.readline().rstrip()
        return cur_dict


def get_stat_df(output_dir):
    base_config = BaseConfig()
    list_df = []
    for cur_path in output_dir.rglob("*"):
        if cur_path.stem == "stat_0":
            output_file_path = OutputFile(cur_path)
            cur_dict = output_file_path.get_stat_dict()
            
            exp_dir_name = cur_path.parent.name 
            split_exp_dir_name = exp_dir_name.split("_")
            t1_size = int(split_exp_dir_name[0].split("=")[1])
            t2_size = int(split_exp_dir_name[1].split("=")[1])
            cur_dict["t1"] = t1_size 
            cur_dict["t2"] = t2_size

            workload_name = cur_path.parent.parent.name
            cur_dict["c_workload"] = workload_name 
            cur_dict["scaled_t1"] = -1 
            cur_dict["scaled_t2"] = -1 
            split_workload_name = workload_name.split("-")
            if len(split_workload_name) == 1:
                cur_dict["workload"] = workload_name
                cur_dict["rate"] = -1 
                cur_dict["seed"] = -1 
                cur_dict["bits"] = -1 
                cur_dict["pp"] = -1 
            elif len(split_workload_name) == 2:
                cur_dict["workload"] = split_workload_name[0]
                split_sample_data = split_workload_name[1].split("_")
                cur_dict["rate"] = int(split_sample_data[0])
                cur_dict["seed"] = int(split_sample_data[1])
                cur_dict["bits"] = int(split_sample_data[2])
                cur_dict["pp"] = -1 
                cur_dict["scaled_t1"] = int(t1_size/(cur_dict["rate"]/100))
                cur_dict["scaled_t2"] = int(t2_size/(cur_dict["rate"]/100))
            else:
                cur_dict["workload"] = split_workload_name[1]
                split_sample_data = split_workload_name[2].split("_")
                cur_dict["rate"] = int(split_sample_data[0])
                cur_dict["seed"] = int(split_sample_data[2])
                cur_dict["bits"] = int(split_sample_data[1])
                cur_dict["pp"] = split_workload_name[0] 
                metric, abits = cur_dict["pp"].split("_")
                num_iter = int(split_workload_name[3])
                pp_output_file_path = base_config.get_sample_post_process_output_file_path("basic",
                                                                                            cur_dict["workload"],
                                                                                            metric,
                                                                                            int(abits),
                                                                                            cur_dict["rate"],
                                                                                            cur_dict["bits"],
                                                                                            cur_dict["seed"])
                pp_output_df = read_csv(pp_output_file_path)
                # print(num_iter)
                # print(cur_path)
                # print(pp_output_file_path)
                # print(pp_output_df)
                eff_rate = pp_output_df.iloc[num_iter]["rate"]
                cur_dict["scaled_t1"] = int(t1_size/eff_rate)
                cur_dict["scaled_t2"] = int(t2_size/eff_rate)
                
            list_df.append(cur_dict)
    return DataFrame(list_df)


def full_workload_replay_row(cur_row, full_df):
    t1_size, t2_size = cur_row["scaled_t1"], cur_row["scaled_t2"]
    for row_index, row in full_df.iterrows():
        if abs(row["t1"] - t1_size) < 2:
            if t2_size == 0 and row["t2"] == 0:
                return row 
            else:
                if abs(row["t2"] - t2_size) < 2:
                    return row 
    return None 


def analyze(df):
    filter_df = df[["workload", "rate", "bits", "seed", "pp", "t1", "scaled_t1", "t2", "scaled_t2", 'blockReadLatency_avg_ns', 'blockWriteLatency_avg_ns']]
    #print(filter_df.to_string())
    df = df[df["blockReadLatency_avg_ns"] > 0]
    for group_name, group_df in df.groupby(["workload"]):
        full_df = group_df[(group_df["scaled_t1"] == -1)]
        sample_df = group_df[(group_df["scaled_t1"] > 0)]
        for c_workload, workload_group_df in sample_df.groupby(['c_workload']):
            #print(workload_group_df[["workload", "rate", "bits", "seed", "pp", "t1", "scaled_t1", "t2", "scaled_t2", 'blockReadLatency_avg_ns', 'blockWriteLatency_avg_ns', 'c_workload', 'readCacheHitRate']])
            for t1_size, t1_size_group_df in workload_group_df.groupby(["scaled_t1"]):
                #print(t1_size_group_df[["workload", "rate", "bits", "seed", "pp", "t1", "scaled_t1", "t2", "scaled_t2", 'blockReadLatency_avg_ns', 'blockWriteLatency_avg_ns', 'c_workload', 'readCacheHitRate']])

                plot_dir = Path("./files/plot/{}/{}".format(c_workload, t1_size))
                plot_dir.mkdir(exist_ok=True, parents=True)
                #print(plot_dir)

                data_dict = defaultdict(list)
                for row_index, row in t1_size_group_df.iterrows():
                    full_row = full_workload_replay_row(row, full_df)

                    if full_row is None:
                        continue 

                    # print(row[["workload", "rate", "bits", "seed", "pp", "t1", "scaled_t1", "t2", "scaled_t2", 'blockReadLatency_avg_ns', 'blockWriteLatency_avg_ns', 'c_workload', 'readCacheHitRate']])
                    # print(full_row[["workload", "rate", "bits", "seed", "pp", "t1", "scaled_t1", "t2", "scaled_t2", 'blockReadLatency_avg_ns', 'blockWriteLatency_avg_ns', 'c_workload', 'readCacheHitRate']])

                    for col_name in list(row.index.to_list()):
                        try:
                            data_dict[col_name].append([row["scaled_t2"], float(row[col_name]), float(full_row[col_name])])
                        except:
                            pass 
                
                # print(c_workload, t1_size)
                # print(data_dict)

                plot_var_list = ["physicalIat", "backingWriteSize", "backingReadSize", "blockReadLatency", 
                                    "blockWriteLatency", "backingReadLatency", "backingWriteLatency", "nvmReadLatency", 
                                    "nvmWriteLatency", "allocateLatency", "findLatency", "bandwidth", "readCacheHitRate", "nvmHitRate"]
                for key in data_dict:
                    # print(any([var_key in key for var_key in plot_var_list]))
                    if not any([var_key in key for var_key in plot_var_list]):
                        continue 

                    plot_path = plot_dir.joinpath("{}.png".format(key))
                    cur_data = data_dict[key]
                    cur_data.sort(key=lambda x: x[0])

                    t2_size_list = [d[0] for d in cur_data]
                    full_val_list = [d[1] for d in cur_data]
                    sample_val_list = [d[2] for d in cur_data]

                    print(plot_path)
                    fig, ax = plt.subplots(figsize=[14,10])
                    ax.plot(t2_size_list, full_val_list, '-*', markersize=15, label="Full")
                    ax.plot(t2_size_list, sample_val_list, '-o', markersize=15, label="Sample")
                    ax.set_ylim(bottom=0)
                    ax.set_xlabel("T2 Size (MB)")
                    ax.set_ylabel(key)
                    ax.set_title("{}, T1 Size MB = {}".format(c_workload, t1_size))
                    ax.legend()
                    plt.savefig(plot_path)
                    plt.close(fig)


def main():
    parser = ArgumentParser(description="Generate DF.")
    parser.add_argument("--machine", "-m", default="c220g5", type=str, help="Machine type.")
    args = parser.parse_args()
    cur_df = get_stat_df(OUTPUT_DIR.joinpath(args.machine))
    cur_df["bandwidth"] = cur_df["totalBlockReqByte"]/cur_df["timeElapsed_sec"]
    cur_df["nvmHitRate"] = (cur_df['numNvmGets'] - cur_df['numNvmGetMiss'])/cur_df['numNvmGets']
    cur_df.to_csv("./files/replay_data/replay.csv", index=False)
    analyze(cur_df)

    

if __name__ == "__main__":
    main()
        
    