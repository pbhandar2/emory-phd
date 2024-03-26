from pathlib import Path 
from argparse import ArgumentParser
from pandas import read_csv 
from numpy import savetxt, inf, zeros, array 
from collections import Counter 

from PyMimircache.cacheReader.csvReader import CsvReader
from PyMimircache.profiler.cLRUProfiler import CLRUProfiler as LRUProfiler

from cydonia.profiler.CacheTrace import CacheTraceReader

from keyuri.config.BaseConfig import BaseConfig


def create_rd_hist(
        cache_trace_path: Path, 
        rd_trace_path: Path, 
        rd_hist_path: Path
) -> None:
    max_rd = -inf 
    read_rd_counter, write_rd_counter = Counter(), Counter()
    cache_trace_reader = CacheTraceReader(cache_trace_path)
    with rd_trace_path.open("r") as rd_trace_handle:
        rd_trace_line = rd_trace_handle.readline().rstrip()
        while rd_trace_line:
            cur_rd_val = int(rd_trace_line)
            cur_cache_req = cache_trace_reader.get_next_cache_req()
            if cur_cache_req[cache_trace_reader._config.op_header_name] == "r":
                read_rd_counter[cur_rd_val] += 1
            elif cur_cache_req[cache_trace_reader._config.op_header_name] == "w":
                write_rd_counter[cur_rd_val] += 1
            else:
                raise ValueError("Unknown OP type {}.".format(cur_cache_req[cache_trace_reader._config.op_header_name]))

            if cur_rd_val > max_rd:
                max_rd = cur_rd_val
            rd_trace_line = rd_trace_handle.readline().rstrip()
        
    assert max_rd > -inf 
    rd_hist_arr = zeros((max_rd + 2, 2), dtype=int)
    rd_hist_arr[0][0], rd_hist_arr[0][1] = int(read_rd_counter[-1]), int(write_rd_counter[-1])
    for cur_rd in range(max_rd + 1):
        rd_hist_arr[cur_rd + 1][0] = int(read_rd_counter[cur_rd])
        rd_hist_arr[cur_rd + 1][1] = int(write_rd_counter[cur_rd])
    
    rd_hist_path.parent.mkdir(exist_ok=True, parents=True)
    savetxt(str(rd_hist_path.absolute()), rd_hist_arr, delimiter=",", fmt="%d")
        

class CreateRDTrace:
    def __init__(
            self, 
            cache_trace_path: Path
    ) -> None:
        self._cache_trace_path = cache_trace_path
    

    def create(
            self, 
            rd_trace_path: Path
    ) -> None:
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
        

def main():
    parser = ArgumentParser(description="Post process sample block traces.")
    parser.add_argument("-t",
                            "--type",
                            type=str,
                            default=None,
                            help="If generating rd trace from sample cache traces.")
    parser.add_argument("-w",
                            "--workload",
                            type=str,
                            required=True, 
                            help="Name of the workload, only if sample type is specified.")
    parser.add_argument("-r",
                            "--rate",
                            type=float,
                            default=None,
                            help="Process files with a specific rate only.")
    parser.add_argument("-b",
                            "--bits",
                            type=int,
                            default=None,
                            help="Number of bits ignored.")
    args = parser.parse_args()

    config = BaseConfig()
    cache_trace_list = config.get_all_sample_cache_traces(args.type, args.workload) if args.type is not None else [config.get_cache_trace_path(args.workload)]
    for cache_trace_path in cache_trace_list:
        if ".rd" in cache_trace_path.name or (args.type == '' and args.workload != cache_trace_path.stem):
            continue 

        if args.type is None:
            rd_trace_path = config.get_rd_trace_path(cache_trace_path.stem)
            rd_hist_path = config.get_rd_hist_file_path(cache_trace_path.stem)
        else:
            split_cache_file_name = cache_trace_path.stem.split('_')
            rate, bits, seed = int(split_cache_file_name[0]), int(split_cache_file_name[1]), int(split_cache_file_name[2])
            rd_trace_path = config.get_sample_rd_trace_path(args.type, args.workload, rate, bits, seed)
            rd_hist_path = config.get_sample_rd_hist_file_path(args.type, args.workload, rate, bits, seed)
        
        if args.rate:
            if int(args.rate*100) != rate:
                continue 
        
        if args.bits and args.bits != bits:
            continue 
        
        if rd_trace_path.exists():
            print("RD trace {} already exists.".format(rd_trace_path))
        else:
            print("Generating RD trace {} from {}.".format(rd_trace_path, cache_trace_path))
            rd_trace = CreateRDTrace(cache_trace_path)
            rd_trace_path.parent.mkdir(exist_ok=True, parents=True)
            rd_trace.create(rd_trace_path)    
            assert rd_trace_path.exists()
        
        if rd_hist_path.exists():
            print("Rd hist {} already exists.".format(rd_hist_path))
        else:
            print("Generating RD hist trace {} from {}.".format(rd_hist_path, cache_trace_path))
            create_rd_hist(cache_trace_path, rd_trace_path, rd_hist_path)
        
        create_rd_hist(cache_trace_path, rd_trace_path, rd_hist_path)


if __name__ == "__main__":
    main()