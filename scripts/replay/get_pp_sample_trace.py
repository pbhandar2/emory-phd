from argparse import ArgumentParser
from pandas import read_csv 

from keyuri.config.BaseConfig import BaseConfig
from keyuri.config.PostProcessConfig import PostProcessFiles


def get_pp_cache_trace(args):
    base_config = BaseConfig()
    post_process_files = PostProcessFiles(base_config)
    file_dict, complete_flag = post_process_files.get_files_for_hit_rate_err_experiment(args.type,
                                                                                            args.workload,
                                                                                            args.metric,
                                                                                            args.abits,
                                                                                            args.rate,
                                                                                            args.bits,
                                                                                            args.seed)


    df = read_csv(file_dict["post_process_output_file_path"])
    num_removal_to_target_rate = len(df[df["rate"] >= args.targetrate])

    pp_cache_trace_path = base_config.get_pp_cache_trace_file_path(args.type, args.workload, args.metric, 
                                                                    args.abits, int(100*args.rate), args.bits, args.seed, num_removal_to_target_rate)
    
    while not pp_cache_trace_path.exists():
        # if pp_cache_trace_path.exists():
        #     print("{}".format(pp_cache_trace_path))
        # else:
        #     print("Does not exist. {}".format(pp_cache_trace_path))
        num_removal_to_target_rate -= 1 
        pp_cache_trace_path = base_config.get_pp_cache_trace_file_path(args.type, args.workload, args.metric, 
                                                                        args.abits, int(100*args.rate), args.bits, args.seed, num_removal_to_target_rate)

    print("{}".format(pp_cache_trace_path))
    print("Rate at {}".format(df.iloc[num_removal_to_target_rate]))

    

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

    get_pp_cache_trace(args)


if __name__ == "__main__":
    main()