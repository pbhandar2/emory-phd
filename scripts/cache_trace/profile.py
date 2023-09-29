

from pathlib import Path 
from argparse import ArgumentParser


from keyuri.config.Config import GlobalConfig
from keyuri.experiments.ProfileCacheTrace import ProfileCacheTrace


def main():
    parser = ArgumentParser(description="Generate a cache trace in a remote node.")
    parser.add_argument("workload_name", type=str, help="Name of the workload")
    parser.add_argument("--workload_type", type=str, default="cp", help="Type of workload.")
    parser.add_argument("--sample_type", type=str, default="iat", help="Type of sample.")
    parser.add_argument("--algo_type", type=str, default="reduce-12", help="Type of algorithm used for post-processing.")
    args = parser.parse_args()

    global_config = GlobalConfig()
    cache_trace_dir_path = global_config.postprocess_sample_cache_trace_dir_path.joinpath(args.algo_type,
                                                                                                args.sample_type,
                                                                                                args.workload_type,
                                                                                                args.workload_name)

    rd_hist_dir_path = global_config.postprocess_rd_hist_dir_path.joinpath(args.algo_type,
                                                                                args.sample_type,
                                                                                args.workload_type,
                                                                                args.workload_name)
    rd_hist_dir_path.mkdir(exist_ok=True, parents=True)
    
    for cache_trace_path in cache_trace_dir_path.iterdir():
        rd_hist_file_path = rd_hist_dir_path.joinpath(cache_trace_path.name)
        print("Profiling {}.".format(cache_trace_path))
        profiler = ProfileCacheTrace(cache_trace_path)
        profiler.create_rd_hist_file(rd_hist_file_path)
        print("Created RD hist file {}.".format(rd_hist_file_path))


if __name__ == "__main__":
    main()