""" Validate the output of post-processing algorithms. """

from argparse import ArgumentParser
from pandas import read_csv 

from keyuri.config.BaseConfig import BaseConfig
from cydonia.profiler.CacheTrace import CacheTraceReader



def main():
    parser = ArgumentParser(description="Validate the output of post-processing algorithm.")

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
                            help="Sampling rate in percentage.")

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
    
    args = parser.parse_args()

    base_config = BaseConfig()

    sample_cache_trace_path = base_config.get_sample_cache_trace_path(args.type,
                                                                        args.workload,
                                                                        int(100*args.rate),
                                                                        args.bits,
                                                                        args.seed)
    
    if not sample_cache_trace_path.exists():
        print("Sample cache trace {} does not exist. Exiting.".format(sample_cache_trace_path))
        return 

    cache_trace_reader = CacheTraceReader(sample_cache_trace_path)
    sample_blk_addr_set = cache_trace_reader.get_unscaled_unique_block_addr_set()
    sample_num_blocks = len(sample_blk_addr_set)

    cp_feature_df = read_csv("../test/block.csv")
    full_num_blocks = int(cp_feature_df[cp_feature_df["workload"]==args.workload]["wss"]/4096)

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
    
    post_process_output_df = read_csv(post_process_output_file_path)
    
    for _, row in post_process_output_df.iterrows():
        relevant_blk_addr_list = CacheTraceReader.get_blk_addr_arr(int(row["addr"]), args.bits)
        num_blk_removed = 0 
        for blk_addr in relevant_blk_addr_list:
            if blk_addr in sample_blk_addr_set:
                num_blk_removed += 1 
        
        assert sample_num_blocks - num_blk_removed == row["block_count"]
        sample_num_blocks -= num_blk_removed
        print("Validated row: {}".format(row))
        

if __name__ == "__main__":
    main()