from pathlib import Path 
from numpy import savetxt
from argparse import ArgumentParser
from pandas import read_csv 

from PyMimircache.cacheReader.csvReader import CsvReader
from PyMimircache.profiler.cLRUProfiler import CLRUProfiler as LRUProfiler

from keyuri.config.BaseConfig import BaseConfig


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
        

def main():
    parser = ArgumentParser(description="Post process sample block traces.")
    parser.add_argument("-t",
                            "--type",
                            type=str,
                            default='',
                            help="If generating rd trace from sample cache traces.")
    parser.add_argument("-w",
                            "--workload",
                            type=str,
                            default='',
                            help="Name of the workload, only if sample type is specified.")
    args = parser.parse_args()

    config = BaseConfig()
    cache_trace_list = config.get_all_cache_traces() if args.type == '' else config.get_all_sample_cache_traces(args.type, args.workload)
    for cache_trace_path in cache_trace_list:
        if ".rd" in cache_trace_path.name or (args.type == '' and args.workload != cache_trace_path.stem):
            continue 

        if args.type == '':
            rd_trace_path = config.get_rd_trace_path(cache_trace_path.stem)
        else:
            split_cache_file_name = cache_trace_path.stem.split('_')
            rate, bits, seed = int(split_cache_file_name[0]), int(split_cache_file_name[1]), int(split_cache_file_name[2])
            rd_trace_path = config.get_sample_rd_trace_path(args.type, args.workload, rate, bits, seed)

        if rd_trace_path.exists():
            print("RD trace {} already exists.".format(rd_trace_path))
            #continue 
        
        print("Generating RD trace {} from {}.".format(rd_trace_path, cache_trace_path))
        rd_trace = CreateRDTrace(cache_trace_path)
        rd_trace_path.parent.mkdir(exist_ok=True, parents=True)
        rd_trace.create(rd_trace_path)    
        assert rd_trace_path.exists()


if __name__ == "__main__":
    main()