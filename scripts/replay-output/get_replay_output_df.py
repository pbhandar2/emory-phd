from pathlib import Path 
from argparse import ArgumentParser
from pandas import DataFrame
from copy import deepcopy

from keyuri.analysis.ReplayDB import ReplayDB
from math import floor, ceil 

DEFAULT_SET_NAME = "set-0"
DEFAULT_MACHINE = "c220g5"
DEFAULT_REPLAY_DIR = Path("/research2/mtc/cp_traces/pranav-phd/replay_output")
DEFAULT_SOURCE_DIR = DEFAULT_REPLAY_DIR.joinpath(DEFAULT_SET_NAME, DEFAULT_MACHINE)
INFO_DICT = {
    "rate": -1,
    "bits": -1,
    "seed": -1,
    "pp_type": "",
    "pp_bits": -1,
    "pp_n": -1,
    "t1_size": -1,
    "t2_size": -1,
    "workload": ""
}


def get_replay_feature_dict(replay_stat_file: Path) -> None:
    replay_feature_dict = {}
    with replay_stat_file.open("r") as handle:
        stat_line = handle.readline().rstrip()
        while stat_line:
            metric, value = stat_line.split("=")
            replay_feature_dict[metric] = float(value)
            stat_line = handle.readline().rstrip()
    return replay_feature_dict


def get_replay_info_dict(output_path: Path):
    info_dict = deepcopy(INFO_DICT)
    info_dict["t1_size"], info_dict["t2_size"] = [int(size_str.split("=")[1]) for size_str in output_path.parent.name.split("_")]
    sample_info = output_path.parent.parent.name.split("-")
    
    if len(sample_info) == 1:
        info_dict["workload"] = sample_info[0]
    elif len(sample_info) == 2:
        info_dict["workload"] = sample_info[0]
        rate, bits, seed = sample_info[1].split("_")
        info_dict["rate"], info_dict["bits"], info_dict["seed"] = int(rate), int(bits), int(seed)
    elif len(sample_info) == 4:
        info_dict["workload"] = sample_info[1]
        rate, bits, seed = sample_info[2].split("_")
        info_dict["rate"], info_dict["bits"], info_dict["seed"] = int(rate), int(bits), int(seed)
        info_dict["pp_type"], pp_bits = sample_info[0].split("_")
        info_dict["pp_bits"] = int(pp_bits)
        info_dict["pp_n"] = int(sample_info[3])
    return info_dict


def load_data(source_dir: Path = DEFAULT_REPLAY_DIR):
    replay_feature_dict_arr = []
    for replay_data_path in source_dir.rglob("*"):
        if replay_data_path.name != "stat_0.out" or replay_data_path.stat().st_size == 0:
            continue 

        replay_dict = get_replay_feature_dict(replay_data_path)
        replay_dict["bandwidth"]  = replay_dict['totalBlockReqByte']/replay_dict['timeElapsed_sec']
        replay_info = get_replay_info_dict(replay_data_path)
        replay_feature_dict_arr.append({**replay_info, **replay_dict})
    return DataFrame(replay_feature_dict_arr)


def eval_pair(sample_row, full_row, header_list):
    err_dict = {}
    for key in header_list:
        if key in INFO_DICT.keys():
            continue 
        
        try:
            full_val = float(full_row[key])
            sample_val = float(sample_row[key])
            diff = abs(full_val - sample_val)

            if full_row[key]:
                percent_diff = 100*diff/full_row[key]
                err_dict[key] = percent_diff
            else:
                err_dict[key] = 0 
        except:
            pass 
    
    for info_key in INFO_DICT:
        err_dict[info_key] = sample_row[info_key]
    
    err_dict["full_t1_size"] = full_row["t1_size"]
    err_dict["full_t2_size"] = full_row["t2_size"]
    return err_dict 


def get_pair_err_df(replay_data_df: DataFrame):
    header_list = replay_data_df.columns
    pair_err_dict_list = []
    for workload, workload_df in replay_data_df.groupby(by=["workload"]):
        full_trace_replay_df = workload_df[workload_df["rate"] == -1]
        sample_trace_df = workload_df[workload_df["rate"] > 0]

        for row_index, sample_row in sample_trace_df.iterrows():
            sampling_ratio = sample_row["rate"]/100
            full_t1_size = floor(sample_row["t1_size"]/sampling_ratio)
            full_t2_size = floor(sample_row["t2_size"]/sampling_ratio)

            if len(full_trace_replay_df[(full_trace_replay_df["t1_size"]==(full_t1_size-1)) & (full_trace_replay_df["t2_size"]==(full_t2_size-1))]):
                rows = full_trace_replay_df[(full_trace_replay_df["t1_size"]==(full_t1_size-1)) & (full_trace_replay_df["t2_size"]==(full_t2_size-1))]
                pair_err_dict_list.append(eval_pair(sample_row, rows.iloc[0], header_list))
            elif len(full_trace_replay_df[(full_trace_replay_df["t1_size"]==(full_t1_size)) & (full_trace_replay_df["t2_size"]==(full_t2_size))]):
                rows = full_trace_replay_df[(full_trace_replay_df["t1_size"]==(full_t1_size)) & (full_trace_replay_df["t2_size"]==(full_t2_size))]
                pair_err_dict_list.append(eval_pair(sample_row, rows.iloc[0], header_list))
            elif len(full_trace_replay_df[full_trace_replay_df["t1_size"]==(full_t1_size+1)]):
                rows = full_trace_replay_df[(full_trace_replay_df["t1_size"]==(full_t1_size+1)) & (full_trace_replay_df["t2_size"]==(full_t2_size+1))]
                pair_err_dict_list.append(eval_pair(sample_row, rows.iloc[0], header_list))

    return DataFrame(pair_err_dict_list)


def get_replay_err_df(source_dir: Path = DEFAULT_SOURCE_DIR):
    data_df = load_data(source_dir)
    return get_pair_err_df(data_df)


def main():
    err_df = get_replay_err_df()

    for group_index, group_df in err_df.groupby(by=["workload", "rate", "bits", "seed"]):
        print(group_index)
        print(len(group_df))
        group_df = group_df.sort_values(by=["t1_size", "t2_size"])
        no_pp_df = group_df[group_df['pp_type'] == ""][['workload', 't1_size', 't2_size', 'rate', 'bits', 'bandwidth', 'blockReadLatency_p99_ns', 'blockWriteLatency_p99_ns', 'pp_type']]
        print(len(no_pp_df))
        print(no_pp_df)
        pp_df = group_df[group_df['pp_type'] != ""][['workload', 't1_size', 't2_size', 'rate', 'bits', 'bandwidth', 'blockReadLatency_p99_ns', 'blockWriteLatency_p99_ns', 'pp_type']]
        print(len(pp_df))
        print(pp_df)


if __name__ == "__main__":
    main()