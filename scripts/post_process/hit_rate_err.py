from argparse import ArgumentParser 
from pandas import read_csv 

from cydonia.profiler.CacheTrace import CacheTraceReader

from keyuri.config.BaseConfig import BaseConfig
from keyuri.config.PostProcessConfig import PostProcessFiles, validate_post_process_output_file, \
                                                get_num_iter_post_processing, get_min_num_iter_post_processing
from keyuri.experiments.HitRateError import HitRateError
from rd_trace import CreateRDTrace, create_rd_hist


def create_post_process_rd_hist(file_dict, args, num_iter):
    output_file_path = file_dict["post_process_output_file_path"]
    pp_df = read_csv(output_file_path)
    assert len(pp_df) >= num_iter, "Number of iterations in post-processing."
    
    # get block addresses to be removed from the sample 
    assert pp_df["addr"].is_unique, "Address removed are not unique. {}.".format(output_file_path)
    pp_removed_set = set(pp_df.iloc[:num_iter]["addr"].to_list())

    # find the dict of block addresses to be sampled
    cache_trace_reader = CacheTraceReader(file_dict["sample_file_path"])
    blk_addr_set = cache_trace_reader.get_unscaled_unique_block_addr_set()

    new_cache_trace_blk_addr_dict = {}
    for blk_addr in blk_addr_set:
        if blk_addr >> args.bits not in pp_removed_set:
            new_cache_trace_blk_addr_dict[blk_addr] = True 
    
    # create new post process cache trace 
    pp_cache_trace_path = file_dict["post_process_cache_trace_path"]
    pp_cache_trace_path.parent.mkdir(exist_ok=True, parents=True)
    cache_trace_reader.sample(new_cache_trace_blk_addr_dict, pp_cache_trace_path)

    # create rd trace and rd hist 
    pp_rd_trace_path = file_dict["post_process_rd_trace_path"]
    print("Creating RD trace {}.".format(pp_rd_trace_path))
    rd_trace_create = CreateRDTrace(pp_cache_trace_path)
    pp_rd_trace_path.parent.mkdir(exist_ok=True, parents=True)
    rd_trace_create.create(pp_rd_trace_path)

    pp_rd_hist_path = file_dict["post_process_rd_hist_path"]
    print("Create RD hist {}.".format(pp_rd_hist_path))
    pp_rd_hist_path.parent.mkdir(exist_ok=True, parents=True)
    create_rd_hist(pp_cache_trace_path,
                    pp_rd_trace_path,
                    pp_rd_hist_path)
        
    
def compute_post_process_hit_rate_error(args):
    base_config = BaseConfig()
    post_process_files = PostProcessFiles(base_config)
    file_dict, complete_flag = post_process_files.get_files_for_hit_rate_err_experiment(args.type,
                                                                                            args.workload,
                                                                                            args.metric,
                                                                                            args.abits,
                                                                                            args.rate,
                                                                                            args.bits,
                                                                                            args.seed)
    
    if not complete_flag:
        print("All required files not there for post processing hit rate error. {}".format(file_dict))
        return 

    if not validate_post_process_output_file(file_dict["post_process_output_file_path"], args.targetrate):
        print("Output file does not meet the target rate.")
        return 
    
    if args.min:
        num_iter = get_min_num_iter_post_processing(file_dict["post_process_output_file_path"], args.targetrate)
    else:
        num_iter = get_num_iter_post_processing(file_dict["post_process_output_file_path"], args.targetrate)
    
    
    print("NUM ITER IS ", num_iter)
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

    post_process_hit_rate_error_file_path = base_config.get_pp_hit_rate_error_file_path(args.type,
                                                                                        args.workload,
                                                                                        args.metric,
                                                                                        args.abits,
                                                                                        int(100*args.rate),
                                                                                        args.bits,
                                                                                        args.seed,
                                                                                        num_iter)
    
    if post_process_hit_rate_error_file_path.exists():
        print("Post process hit rate error file {} already exists.".format(post_process_hit_rate_error_file_path))
    else:
        file_dict["post_process_cache_trace_path"] = post_process_cache_trace_path
        file_dict["post_process_rd_trace_path"] = post_process_rd_trace_path
        file_dict["post_process_rd_hist_path"] = post_process_rd_hist_path

        if not post_process_rd_hist_path.exists():
            create_post_process_rd_hist(file_dict, args, num_iter)
    
        print("Computing hit rate error in file {}.".format(post_process_hit_rate_error_file_path))
        hit_rate_error = HitRateError(file_dict["full_rd_hist_file_path"], post_process_rd_hist_path, args.rate)
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
                            default="mean",
                            help="The metric used by the post-processing algorithm.")

    parser.add_argument("--rate",
                            "-r",
                            type=float, 
                            help="Sampling rate between (0.0 and 1.0).")

    parser.add_argument("--seed",
                            "-s",
                            default=42,
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
    
    parser.add_argument("-mi", "--min",
                            action='store_true',
                            help="The flag indicating to select min error.")
    
    args = parser.parse_args()

    compute_post_process_hit_rate_error(args)


if __name__ == "__main__":
    main()