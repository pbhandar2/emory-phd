from pathlib import Path 
from pandas import read_csv
from argparse import ArgumentParser
from numpy import savetxt
from pandas import DataFrame 
from json import loads, load

from cydonia.blksample.sample import generate_sample_cache_trace
from cydonia.blksample.lib import get_feature_err_dict
from cydonia.profiler.CacheTraceProfiler import load_cache_trace, get_workload_feature_dict_from_cache_trace
from keyuri.config.BaseConfig import BaseConfig
from keyuri.analysis.PlotMRC import get_hrc, hrc_mae


from PyMimircache.cacheReader.csvReader import CsvReader
from PyMimircache.profiler.cLRUProfiler import CLRUProfiler as LRUProfiler

class CreateRDTrace:
    def __init__(self, cache_trace_path: Path):
        self._cache_trace_path = cache_trace_path
    

    def create(self, rd_trace_path: Path):
        init_params = {
            "label": 3
        }
        csv_reader = CsvReader(str(self._cache_trace_path), init_params=init_params)
        lru_profiler = LRUProfiler(csv_reader)
        reuse_distance_arr = lru_profiler.get_reuse_distance()
        rd_arr = reuse_distance_arr.astype(int)
        savetxt(str(rd_trace_path), rd_arr, fmt='%i', delimiter='\n')

        rd_trace_df = read_csv(rd_trace_path, names=['rd'])
        cache_trace_df = read_csv(self._cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])

        assert len(rd_trace_df) == len(cache_trace_df), \
            "Rd trace len {} and cache trace len {} not equal".format(len(rd_trace_df), len(cache_trace_df))


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
    


def create_rd_data(
        sample_cache_trace_path: Path,
        post_process_blocks_removed_list: list,
        rd_trace_path: Path,
        rd_hist_path: Path 
) -> None:
    print("Create rd trace {} by removing {} blocks from sample cache trace {}.".format(rd_trace_path, 
                                                                                            len(post_process_blocks_removed_list), 
                                                                                            sample_cache_trace_path))
    
    temp_cache_trace_path = Path("/dev/shm/{}-{}".format(sample_cache_trace_path.parent.name, sample_cache_trace_path.name))
    with sample_cache_trace_path.open("r") as cache_trace_handle,\
            temp_cache_trace_path.open("w+") as new_cache_trace_handle:
        trace_line = cache_trace_handle.readline().rstrip()
        while trace_line:
            block_addr = int(trace_line.split(',')[2])
            if block_addr not in post_process_blocks_removed_list:
                new_cache_trace_handle.write("{}\n".format(trace_line))
            trace_line = cache_trace_handle.readline().rstrip()
    
    create_rd_trace = CreateRDTrace(temp_cache_trace_path)
    create_rd_trace.create(rd_trace_path)
    print("Completed rd trace creation!")

    generate_rd_hist(temp_cache_trace_path, rd_trace_path, rd_hist_path)

    
def main():
    parser = ArgumentParser(description="Analyze output file of post-processing algorithm.")
    parser.add_argument("workload_name", type=str, help="The name of the workload.")
    parser.add_argument("rate", type=int, help="The rate of sampling.")
    parser.add_argument("seed", type=int, help="Random seed.")
    parser.add_argument("--sample_type", type=str, default="iat", help="The type of sampling technique used.")
    parser.add_argument("--algo_name", type=str, default="best", help="The post-processing algorithm used.")
    args = parser.parse_args()

    config = BaseConfig()
    algo_output_dir_path = config.get_algo_output_dir_path(args.algo_name, args.sample_type)

    table_arr = []
    for workload_output_dir_path in algo_output_dir_path.iterdir():
        workload_name = workload_output_dir_path.name 
        if workload_name != args.workload_name:
            continue 
        for output_file_path in workload_output_dir_path.iterdir():
            split_output_file_name = output_file_path.stem.split("_")
            rate, seed = int(split_output_file_name[0]), int(split_output_file_name[2])
            if (rate != args.rate) or (seed != args.seed):
                continue 

            algo_output_df = read_csv(output_file_path)
            min_mean_percent_err_dict = loads(algo_output_df.iloc[-1].to_json())
            min_mean_percent_err = float(algo_output_df.iloc[-1]["mean"])

            start_err = float(algo_output_df.iloc[0]["mean"])

            bits, algo_bits = int(split_output_file_name[1]), int(split_output_file_name[3])
            sample_cache_trace_path = config.get_sample_cache_trace_path(args.sample_type, args.workload_name, rate, bits, seed)
            rd_trace_path = config.get_algo_best_rd_trace_path(args.algo_name, args.sample_type, args.workload_name, rate, bits, seed, algo_bits)
            full_rd_hist_trace_path = config.get_rd_hist_file_path(args.workload_name)
            rd_hist_trace_path = config.get_algo_best_rd_hist_path(args.algo_name, args.sample_type, args.workload_name, rate, bits, seed, algo_bits)
            # if not rd_trace_path.exists() or not rd_hist_trace_path.exists():
            #     rd_trace_path.parent.mkdir(exist_ok=True, parents=True)
            #     create_rd_data(sample_cache_trace_path, algo_output_df["addr"].to_numpy(dtype=int), rd_trace_path, rd_hist_trace_path)

            full_cache_trace_path = config.get_cache_trace_path(args.workload_name)
            sample_cache_trace_path = config.get_sample_cache_trace_path(args.sample_type, args.workload_name, rate, bits, seed)

            read_hrc, write_hrc, overall_hrc = get_hrc(rd_hist_trace_path)
            full_read_hrc, full_write_hrc, full_overall_hrc = get_hrc(full_rd_hist_trace_path)
            min_mean_percent_err_dict['read_hr_mae'], _ = hrc_mae(full_read_hrc, read_hrc, rate)
            min_mean_percent_err_dict['write_hr_mae'], _ = hrc_mae(full_write_hrc, write_hrc, rate)
            min_mean_percent_err_dict['overall_hr_mae'], _ = hrc_mae(full_overall_hrc, overall_hrc, rate)

            min_mean_percent_err_dict['start'] = start_err
            min_mean_percent_err_dict['rate'] = rate
            min_mean_percent_err_dict['seed'] = seed 
            min_mean_percent_err_dict['bits'] = bits
            min_mean_percent_err_dict['algo_bits'] = algo_bits
            min_mean_percent_err_dict['size'] = sample_cache_trace_path.stat().st_size/full_cache_trace_path.stat().st_size

            del min_mean_percent_err_dict["time"]
            del min_mean_percent_err_dict["addr"]
            table_arr.append(min_mean_percent_err_dict)

    df = DataFrame(table_arr)
    print(df.sort_values(by=['mean']))




if __name__ == "__main__":
    main()