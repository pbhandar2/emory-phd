from collections import Counter 
from pathlib import Path 
from argparse import ArgumentParser
from numpy import savetxt, inf, zeros

from cydonia.profiler.CacheTrace import CacheTraceReader

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


def main():
    parser = ArgumentParser(description="Compute RD trace.")
    parser.add_argument("cache_trace_file", type=Path, help="Cache trace file.")
    parser.add_argument("rd_trace_file", type=Path, help="RD trace file.")
    parser.add_argument("rd_hist_file", type=Path, help="The RD hist file.")
    args = parser.parse_args()

    create_rd_hist(args.cache_trace_file, args.rd_trace_file, args.rd_hist_file)


if __name__ == "__main__":
    main()