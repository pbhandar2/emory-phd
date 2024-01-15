"""This script generates hit rate error values for post process samples."""
from json import dump, dumps 
from argparse import ArgumentParser

from keyuri.config.Config import GlobalConfig
from keyuri.experiments.ProfileCacheTrace import ProfileCacheTrace, get_hrc_err_dict, ProfileRDHistogram, HRCType


def main():
    parser = ArgumentParser(description="Generate a cache trace in a remote node.")
    parser.add_argument("workload_name", type=str, help="Name of the workload")
    parser.add_argument("--workload_type", type=str, default="cp", help="Type of workload.")
    parser.add_argument("--sample_type", type=str, default="iat", help="Type of sample.")
    parser.add_argument("--algo_type", type=str, default="reduce-12", help="Type of algorithm used for post-processing.")
    parser.add_argument("--trace_type", 
                            type=str, 
                            default="cache", 
                            help="Type of trace to generate cache or access.")
    args = parser.parse_args()

    type_arr = [HRCType.OVERALL, HRCType.READ, HRCType.READ_ONLY]

    global_config = GlobalConfig()

    sample_rd_hist_dir_path = global_config.sample_rd_hist_dir_path.joinpath(args.algo_type,
                                                                                args.sample_type,
                                                                                args.workload_type,
                                                                                args.workload_name)
    
    sample_cache_trace_dir_path = global_config.sample_cache_trace_dir_path.joinpath(args.sample_type,
                                                                                        args.workload_type,
                                                                                        args.workload_name)
    
    if args.trace_type == "cache":

        
        rd_hist_dir_path = global_config.postprocess_rd_hist_dir_path.joinpath(args.algo_type,
                                                                                    args.sample_type,
                                                                                    args.workload_type,
                                                                                    args.workload_name)
        
        hrc_err_dir_path = global_config.postprocess_hrc_err_dir_path.joinpath(args.algo_type,
                                                                                    args.sample_type,
                                                                                    args.workload_type,
                                                                                    args.workload_name)
    else:
        # sample_rd_hist_dir_path = global_config.sample_access_rd_hist_dir_path.joinpath(args.algo_type,
        #                                                                                     args.sample_type,
        #                                                                                     args.workload_type,
        #                                                                                     args.workload_name)

        # sample_cache_trace_dir_path = global_config.sample_block_access_trace_dir_path.joinpath(args.sample_type,
        #                                                                                             args.workload_type,
        #                                                                                             args.workload_name)
        
        rd_hist_dir_path = global_config.postprocess_access_rd_hist_dir_path.joinpath(args.algo_type,
                                                                                        args.sample_type,
                                                                                        args.workload_type,
                                                                                        args.workload_name)
        
        hrc_err_dir_path = global_config.postprocess_access_hrc_err_dir_path.joinpath(args.algo_type,
                                                                                        args.sample_type,
                                                                                        args.workload_type,
                                                                                        args.workload_name)

    full_trace_rd_hist_path = global_config.get_rd_hist_file_path(args.workload_type, args.workload_name)
    if not full_trace_rd_hist_path.exists():
        full_cache_trace_path = global_config.get_block_cache_trace_path(args.workload_type, args.workload_name)
        if not full_cache_trace_path.exists():
            raise ValueError("Full cache trace does not exist {}.".format(full_cache_trace_path))
        profiler = ProfileCacheTrace(full_cache_trace_path)
        profiler.create_rd_hist_file(full_trace_rd_hist_path)
    
    full_rd_hist_profiler = ProfileRDHistogram(full_trace_rd_hist_path)
    for rd_hist_path in rd_hist_dir_path.iterdir():
        hrc_err_path = hrc_err_dir_path.joinpath(rd_hist_path.name)
        file_name_split = rd_hist_path.stem.split("_")
        rate = int(file_name_split[0])

        sample_cache_trace_path = sample_cache_trace_dir_path.joinpath(rd_hist_path.name)
        if not sample_cache_trace_path.exists():
            print("Sample cache trace {} does not exist.".format(sample_cache_trace_path))
            continue 

        only_sample_rd_hist_file_path = sample_rd_hist_dir_path.joinpath(rd_hist_path.name)
        if not only_sample_rd_hist_file_path.exists():
            profiler = ProfileCacheTrace(sample_cache_trace_path)
            profiler.create_rd_hist_file(only_sample_rd_hist_file_path)

        sample_rd_hist_profiler = ProfileRDHistogram(rd_hist_path)
        only_sample_rd_hist_profiler = ProfileRDHistogram(only_sample_rd_hist_file_path)
        final_err_dict = {}
        for hrc_type in type_arr:
            full_hrc_arr = full_rd_hist_profiler.get_hrc(hrc_type=hrc_type)
            sample_hrc_arr = sample_rd_hist_profiler.get_hrc(hrc_type=hrc_type)
            only_sample_hrc_arr = only_sample_rd_hist_profiler.get_hrc(hrc_type=hrc_type)
            hrc_error_dict = get_hrc_err_dict(full_hrc_arr, sample_hrc_arr, rate)
            sample_error_dict = get_hrc_err_dict(full_hrc_arr, only_sample_hrc_arr, rate)
            final_err_dict["post_{}".format(hrc_type.value)] = hrc_error_dict
            final_err_dict["samp_{}".format(hrc_type.value)] = sample_error_dict
        
        print(only_sample_rd_hist_file_path)
        print(dumps(final_err_dict, indent=2))
        hrc_err_path.parent.mkdir(exist_ok=True, parents=True)
        with hrc_err_path.open("w+") as err_handle:
            dump(final_err_dict, err_handle, indent=2)


if __name__ == "__main__":
    main()