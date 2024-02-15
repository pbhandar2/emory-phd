from argparse import ArgumentParser
from pathlib import Path 
from pandas import read_csv 
from numpy import linspace, unique 

from keyuri.config.BaseConfig import BaseConfig
from keyuri.experiments.HitRateError import HitRateError
from cydonia.profiler.CacheTrace import CacheTraceReader

from rd_trace import CreateRDTrace, create_rd_hist


def compute_hit_rate_error_for_n(base_config, 
                                 args,
                                 num_iter):
    
    full_rd_hist_path = base_config.get_rd_hist_file_path(args.workload)

    post_process_output_file_path = base_config.get_sample_post_process_output_file_path(args.type,
                                                                                            args.workload,
                                                                                            args.metric,
                                                                                            args.abits,
                                                                                            int(100*args.rate),
                                                                                            args.bits,
                                                                                            args.seed)
    

    
    sample_cache_trace_path = base_config.get_sample_cache_trace_path(args.type,
                                                                        args.workload,
                                                                        int(100*args.rate),
                                                                        args.bits,
                                                                        args.seed)
    

    # the hit rate error file to be generated 
    post_process_hit_rate_error_file_path = base_config.get_pp_hit_rate_error_file_path(args.type,
                                                                                        args.workload,
                                                                                        args.metric,
                                                                                        args.abits,
                                                                                        int(100*args.rate),
                                                                                        args.bits,
                                                                                        args.seed,
                                                                                        num_iter)

    if post_process_hit_rate_error_file_path.exists():
        print("Hit rate error file {} already exists. Exiting.".format(post_process_hit_rate_error_file_path))
        return 


    post_process_cache_trace_path = base_config.get_pp_cache_trace_file_path(args.type,
                                                                                args.workload,
                                                                                args.metric,
                                                                                args.abits,
                                                                                int(100*args.rate),
                                                                                args.bits,
                                                                                args.seed,
                                                                                num_iter)

    post_process_rd_trace_path = base_config.get_pp_rd_trace_file_path(args.type,
                                                                                    args.workload,
                                                                                    args.metric,
                                                                                    args.abits,
                                                                                    int(100*args.rate),
                                                                                    args.bits,
                                                                                    args.seed,
                                                                                    num_iter)
    
    post_process_rd_hist_path = base_config.get_pp_rd_hist_file_path(args.type,
                                                                                    args.workload,
                                                                                    args.metric,
                                                                                    args.abits,
                                                                                    int(100*args.rate),
                                                                                    args.bits,
                                                                                    args.seed,
                                                                                    num_iter)
    
    print("Processing")
    print(post_process_output_file_path)
    print(post_process_cache_trace_path)
    print(post_process_rd_trace_path)
    print(post_process_rd_hist_path)
    print(post_process_hit_rate_error_file_path)

    compute_post_process_hit_rate_error(sample_cache_trace_path,
                                            post_process_output_file_path,
                                            post_process_cache_trace_path,
                                            post_process_rd_trace_path,
                                            post_process_rd_hist_path,
                                            post_process_hit_rate_error_file_path,
                                            full_rd_hist_path,
                                            args.abits,
                                            args.rate,
                                            num_iter)


def compute_post_process_hit_rate_error(
        sample_cache_trace_path: Path,
        post_process_output_file_path: Path,
        post_process_cache_trace_path: Path,
        post_process_rd_trace_path: Path,
        post_process_rd_hist_path: Path, 
        post_process_hit_rate_error_file_path: Path,
        full_rd_hist_path: Path, 
        num_lower_addr_bits_ignored: int,
        sampling_rate: float,
        num_iter: int 
) -> None:
    print("""Compute post process hit rate error for 
            sample: {}
            pp_output_file: {}
            pp_cache_trace_path: {}
            pp_rd_trace_path: {}
            pp_rd_hist_trace_path: {}
            pp_hit_rate_error_file_path: {}""".format(sample_cache_trace_path,
                                                        post_process_output_file_path,
                                                        post_process_cache_trace_path,
                                                        post_process_rd_trace_path,
                                                        post_process_rd_hist_path,
                                                        post_process_hit_rate_error_file_path))
    
    pp_df = read_csv(post_process_output_file_path)
    if len(pp_df) < num_iter:
        print("Number of iteration {} too large for pp output with {} iterations.".format(num_iter, len(pp_df)))
        return 
    
    print("All address in post process output are unique: {}".format(pp_df["addr"].is_unique))
    assert pp_df["addr"].is_unique, "Address removed are not unique in post process output file {}.".format(post_process_output_file_path)
    pp_removed_set = set(pp_df.iloc[:num_iter]["addr"].to_list())

    cache_trace_reader = CacheTraceReader(sample_cache_trace_path)
    blk_addr_set = cache_trace_reader.get_unscaled_unique_block_addr_set()

    # find the dict of block addresses to be sampled
    new_cache_trace_blk_addr_dict = {}
    for blk_addr in blk_addr_set:
        if blk_addr >> num_lower_addr_bits_ignored not in pp_removed_set:
            new_cache_trace_blk_addr_dict[blk_addr] = True 
    
    print("Creating new cache trace from post processing at {}.".format(post_process_cache_trace_path))
    post_process_cache_trace_path.parent.mkdir(exist_ok=True, parents=True)
    cache_trace_reader.sample(new_cache_trace_blk_addr_dict, post_process_cache_trace_path)

    print("Creating RD trace {}.".format(post_process_rd_trace_path))
    rd_trace_create = CreateRDTrace(post_process_cache_trace_path)
    post_process_rd_trace_path.parent.mkdir(exist_ok=True, parents=True)
    rd_trace_create.create(post_process_rd_trace_path)

    print("Create RD hist {}.".format(post_process_rd_hist_path))
    post_process_rd_hist_path.parent.mkdir(exist_ok=True, parents=True)
    create_rd_hist(post_process_cache_trace_path,
                    post_process_rd_trace_path,
                    post_process_rd_hist_path)
    
    print("Computing hit rate error in file {}.".format(post_process_hit_rate_error_file_path))
    hit_rate_error = HitRateError(full_rd_hist_path, post_process_rd_hist_path, sampling_rate)
    hit_rate_error_df = hit_rate_error.get_hit_rate_err_df()
    post_process_hit_rate_error_file_path.parent.mkdir(exist_ok=True, parents=True)
    hit_rate_error_df.to_csv(post_process_hit_rate_error_file_path, index=False)
    print("Done.")


def main():
    parser = ArgumentParser(description="Compute the hit rate error of post processing algorithm.")

    parser.add_argument("--workload",
                            "-w", 
                            type=str, 
                            help="Name of workload.")

    parser.add_argument("--type",
                            "-t", 
                            type=str, 
                            default="basic",
                            help="The sample type.")
    
    parser.add_argument("--metric",
                            "-m", 
                            type=str, 
                            help="The metric used by the post-processing algorithm.")

    parser.add_argument("--rate",
                            "-r",
                            type=float, 
                            help="Sampling rate between (0 and 1).")

    parser.add_argument("--seed",
                            "-s",
                            type=int, 
                            help="Random seed of the sample.")

    parser.add_argument("--bits",
                            "-b",
                            type=int, 
                            help="Number of lower order bits of addresses ignored.")
    
    parser.add_argument("--abits",
                            "-a",
                            type=int, 
                            help="Number of lower order bits of addresses ignored.")

    parser.add_argument("--targetrate",
                            "-tr",
                            type=float, 
                            help="Target sampling rate.")
    
    parser.add_argument("--numpoints",
                            "-n",
                            default=10,
                            type=int,
                            help="The number of points to evaluate.")
    
    args = parser.parse_args()

    base_config = BaseConfig()

    # the output file of the post-processing algorithm
    post_process_output_file_path = base_config.get_sample_post_process_output_file_path(args.type,
                                                                                            args.workload,
                                                                                            args.metric,
                                                                                            args.abits,
                                                                                            int(100*args.rate),
                                                                                            args.bits,
                                                                                            args.seed)
    
    if not post_process_output_file_path.exists():
        print("Post process output file {} does not exists. Exiting.".format(post_process_output_file_path))
        return 
    
    sample_cache_trace_path = base_config.get_sample_cache_trace_path(args.type,
                                                                        args.workload,
                                                                        int(100*args.rate),
                                                                        args.bits,
                                                                        args.seed)
    
    if not sample_cache_trace_path.exists():
        print("Sample cache trace {} does not exist. Exiting.".format(sample_cache_trace_path))
        return 
    
    output_df = read_csv(post_process_output_file_path)
    # extract the first row where where the hit rate is less than or equal to the target rate 
    min_rate = output_df["rate"].min()

    if min_rate <= args.targetrate:
        first_row = output_df.loc[output_df['rate'] <= args.targetrate].iloc[0]
        output_df = output_df[output_df["block_count"] >= first_row["block_count"]]

    num_iter_arr = unique(linspace(1, len(output_df), args.numpoints, dtype=int))
    full_rd_hist_path = base_config.get_rd_hist_file_path(args.workload)

    print("array of num iterations")
    print(num_iter_arr)

    for num_iter in num_iter_arr:
        # the hit rate error file to be generated 
        post_process_hit_rate_error_file_path = base_config.get_pp_hit_rate_error_file_path(args.type,
                                                                                            args.workload,
                                                                                            args.metric,
                                                                                            args.abits,
                                                                                            int(100*args.rate),
                                                                                            args.bits,
                                                                                            args.seed,
                                                                                            num_iter)
    
        if post_process_hit_rate_error_file_path.exists():
            print("Hit rate error file {} already exists. Exiting.".format(post_process_hit_rate_error_file_path))
            continue 

    
        post_process_cache_trace_path = base_config.get_pp_cache_trace_file_path(args.type,
                                                                                    args.workload,
                                                                                    args.metric,
                                                                                    args.abits,
                                                                                    int(100*args.rate),
                                                                                    args.bits,
                                                                                    args.seed,
                                                                                    num_iter)
    
        post_process_rd_trace_path = base_config.get_pp_rd_trace_file_path(args.type,
                                                                                        args.workload,
                                                                                        args.metric,
                                                                                        args.abits,
                                                                                        int(100*args.rate),
                                                                                        args.bits,
                                                                                        args.seed,
                                                                                        num_iter)
        
        post_process_rd_hist_path = base_config.get_pp_rd_hist_file_path(args.type,
                                                                                        args.workload,
                                                                                        args.metric,
                                                                                        args.abits,
                                                                                        int(100*args.rate),
                                                                                        args.bits,
                                                                                        args.seed,
                                                                                        num_iter)
        
        print("Processing")
        print(post_process_output_file_path)
        print(post_process_cache_trace_path)
        print(post_process_rd_trace_path)
        print(post_process_rd_hist_path)
        print(post_process_hit_rate_error_file_path)
    
        compute_post_process_hit_rate_error(sample_cache_trace_path,
                                                post_process_output_file_path,
                                                post_process_cache_trace_path,
                                                post_process_rd_trace_path,
                                                post_process_rd_hist_path,
                                                post_process_hit_rate_error_file_path,
                                                full_rd_hist_path,
                                                args.abits,
                                                args.rate,
                                                num_iter)


if __name__ == "__main__":
    main()