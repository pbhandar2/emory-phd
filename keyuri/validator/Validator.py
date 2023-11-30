from pathlib import Path 
from pandas import read_csv 

from cydonia.profiler.RDHistogram import RDHistogram


def validate_all_rd_hist(
        rd_hist_dir_path: Path,
        cache_trace_dir_path: Path 
) -> None:
    for rd_hist_file_path in rd_hist_dir_path.iterdir():
        cache_trace_file_path = cache_trace_dir_path.joinpath(rd_hist_file_path.name )
        validate_rd_hist(rd_hist_file_path, cache_trace_file_path)


def validate_rd_hist(
        rd_hist_file_path: Path,
        cache_trace_file_path: Path
) -> None:
    rd_hist = RDHistogram()
    rd_hist.load_rd_hist_file(rd_hist_file_path)
    cache_trace_df = read_csv(cache_trace_file_path, names=["ts", "blk", "op", "rd"])

    assert rd_hist.read_count == len(cache_trace_df[cache_trace_df["op"] == 'r']),\
        "The read request count in RD histogram {} did not match the cache trace {}.".format(rd_hist.read_count, len(cache_trace_df[cache_trace_df["op"] == 'r']))
    assert rd_hist.write_count == len(cache_trace_df[cache_trace_df["op"] == 'w']),\
        "The read request count in RD histogram {} did not match the cache trace {}.".format(rd_hist.write_count, len(cache_trace_df[cache_trace_df["op"] == 'w']))
    
    rd_hist_infinite_rd_read_count = rd_hist.read_counter[rd_hist.infinite_rd_val]
    trace_infinite_rd_read_count = len(cache_trace_df[(cache_trace_df["rd"] == rd_hist.infinite_rd_val) & (cache_trace_df["op"] == 'r')])
    rd_hist_infinite_rd_write_count = rd_hist.write_counter[rd_hist.infinite_rd_val]
    trace_infinite_rd_write_count = len(cache_trace_df[(cache_trace_df["rd"] == rd_hist.infinite_rd_val) & (cache_trace_df["op"] == 'w')])

    assert rd_hist_infinite_rd_read_count == trace_infinite_rd_read_count,\
        "The number of cold read miss in RD histogram {} and cache trace {} did not match.".format(rd_hist_infinite_rd_read_count, trace_infinite_rd_read_count)
    assert rd_hist_infinite_rd_write_count == trace_infinite_rd_write_count,\
        "The number of cold write miss in RD histogram {} and cache trace {} did not match.".format(rd_hist_infinite_rd_write_count, trace_infinite_rd_write_count)
    