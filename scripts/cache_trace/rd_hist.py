from pathlib import Path 
from argparse import ArgumentParser 
from pandas import read_csv 

from keyuri.config.BaseConfig import BaseConfig

from rd_trace import CreateRDTrace


def generate_rd_hist(
        cache_trace_path: Path,
        rd_trace_path: Path,
        rd_hist_trace_path: Path 
) -> None:
    cache_trace_df = read_csv(cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])

    print("Generating cache trace {}, rd trace {} and rd hist trace {}".format(cache_trace_path, rd_trace_path, rd_hist_trace_path))
    rd_trace_df = read_csv(rd_trace_path, names=["rd"])

    assert len(cache_trace_df) == len(rd_trace_df), \
        "Number of lines in cache {} and rd trace {} not equal.".format(len(cache_trace_df), len(rd_trace_df))
    
    cache_trace_df["rd"] = rd_trace_df["rd"]
    read_rd_value_counts = cache_trace_df[cache_trace_df["op"] == "r"]["rd"].value_counts()
    write_rd_value_counts = cache_trace_df[cache_trace_df["op"] == "w"]["rd"].value_counts()
    rd_hist_trace_path.parent.mkdir(exist_ok=True, parents=True)
    with rd_hist_trace_path.open("w+") as rd_hist_handle:
        max_rd = cache_trace_df["rd"].max()
        for cur_rd in range(-1, max_rd+1):
            read_rd_count, write_rd_count = 0, 0 
            if cur_rd in read_rd_value_counts:
                read_rd_count = read_rd_value_counts[cur_rd]
            
            if cur_rd in write_rd_value_counts:
                write_rd_count = write_rd_value_counts[cur_rd]
            
            rd_hist_handle.write("{},{}\n".format(read_rd_count, write_rd_count))
    
    rd_hist_df = read_csv(rd_hist_trace_path, names=['r', 'w'])
    total_req_rd_hist = rd_hist_df['r'].sum() + rd_hist_df['w'].sum()
    assert total_req_rd_hist == len(cache_trace_df), \
        "Number of requests in rd hist {} is not matching the number of requests in cache trace {}.".format(total_req_rd_hist, len(cache_trace_df))
    

def main():
    parser = ArgumentParser(description="Generate reuse distance histograms.")
    parser.add_argument("-w",
                            "--workload",
                            type=str,
                            help="The name of the workload.")
    parser.add_argument("-t",
                            "--type",
                            type=str,
                            default='',
                            help="The name of the workload.")
    args = parser.parse_args()

    config = BaseConfig()

    if len(args.type) == 0:
        cache_trace_path = config.get_cache_trace_path(args.workload)
        rd_trace_path = config.get_rd_trace_path(args.workload)
        rd_hist_trace_path = config.get_rd_hist_file_path(args.workload)
        generate_rd_hist(cache_trace_path, rd_trace_path, rd_hist_trace_path)
    else:
        cache_trace_list = config.get_all_sample_cache_traces(args.type, args.workload)
        for cache_trace_path in cache_trace_list:
            if '.rd' in cache_trace_path.name:
                continue 

            split_cache_trace_file_name = cache_trace_path.stem.split('_')
            rate, bits, seed = int(split_cache_trace_file_name[0]), int(split_cache_trace_file_name[1]), int(split_cache_trace_file_name[2])

            rd_trace_path = config.get_sample_rd_trace_path(args.type, args.workload, rate, bits, seed)
            rd_hist_path = config.get_sample_rd_hist_file_path(args.type, args.workload, rate, bits, seed)
            rd_hist_path.parent.mkdir(exist_ok=True, parents=True)

            try:
                generate_rd_hist(cache_trace_path, rd_trace_path, rd_hist_path)
            except Exception as e:
                rd_trace = CreateRDTrace(cache_trace_path)
                rd_trace_path.parent.mkdir(exist_ok=True, parents=True)
                rd_trace.create(rd_trace_path)    
                print("Error generating")
                print(e)
                print("recreated file")
                generate_rd_hist(cache_trace_path, rd_trace_path, rd_hist_path)


if __name__ == "__main__":
    main()