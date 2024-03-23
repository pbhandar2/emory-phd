"""This module computes error in workload features by comparing
the full and sample traces.
"""

from pathlib import Path 
from pandas import read_csv 
from numpy import inf, zeros, savetxt
from collections import Counter 

from cydonia.profiler.CacheTrace import CacheTraceReader
from PyMimircache.cacheReader.csvReader import CsvReader
from PyMimircache.profiler.cLRUProfiler import CLRUProfiler as LRUProfiler


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