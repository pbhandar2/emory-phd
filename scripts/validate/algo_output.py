""" Plot feature error vs number of address bits ignored for a given workload and sample rate.
"""

from argparse import ArgumentParser
from pandas import DataFrame, read_csv 
from json import load

from keyuri.config.BaseConfig import BaseConfig
from keyuri.tracker.sample import check_if_sample_error_set, get_sample_error_file_list

from cydonia.profiler.CacheTrace import CacheTraceReader
from cydonia.profiler.BAFM import BAFM 


def main():
    parser = ArgumentParser(description="Plot post processing output.")
    parser.add_argument("-w", 
                            "--workload", 
                            type=str, 
                            help="Name of workload.")
    parser.add_argument("-t",
                            "--type",
                            type=str,
                            help="The type of sample.")
    parser.add_argument("-r",
                            "--rate",
                            type=float,
                            help="Sampling Rate.")
    parser.add_argument("-s",
                            "--seed",
                            type=int,
                            help="Random seed.")
    parser.add_argument("-b",
                            "--bits",
                            type=int,
                            help="Number of lower order bits of block keys ignored during sampling.")
    parser.add_argument("-ab",
                            "--abits",
                            type=int,
                            help="Number of lower order bits of block keys ignored during post processing.")
    parser.add_argument("-a",
                            "--algo",
                            type=str,
                            help="The algorithm type.")
    args = parser.parse_args()


    base_config = BaseConfig()
    output_path = base_config.get_sample_post_process_output_file_path(args.type,
                                                                        args.workload,
                                                                        args.algo,
                                                                        args.abits,
                                                                        int(100 * args.rate),
                                                                        args.bits,
                                                                        args.seed)
    
    cache_trace_path = base_config.get_sample_cache_trace_path(args.type,
                                                                args.workload,
                                                                int(args.rate * 100),
                                                                args.bits,
                                                                args.seed)

    if output_path.exists():
        print("Found output {}.".format(output_path))
        output_df = read_csv(output_path)
        print(output_df)

        sample_error_file_path = base_config.get_sample_block_error_file_path(args.type,
                                                                                args.workload,
                                                                                int(100 * args.rate),
                                                                                args.bits,
                                                                                args.seed)
        
        with sample_error_file_path.open("r") as handle:
            sample_error_json = load(handle)

        print(sample_error_json)

        cache_feature_file_path = base_config.get_sample_cache_features_path(args.type, args.workload, int(100 * args.rate), args.bits, args.seed)
        with cache_feature_file_path.open("r") as handle:
            cache_feature_dict = load(handle)

        full_cache_feature_file_path = base_config.get_cache_features_path(args.workload)
        with full_cache_feature_file_path.open("r") as handle:
            full_cache_feature_dict = load(handle)

        print(cache_feature_dict)

        print(full_cache_feature_file_path)

        new_err_dict = BAFM.get_error_dict(full_cache_feature_dict, cache_feature_dict)

        print(new_err_dict)



        cache_trace_reader = CacheTraceReader(cache_trace_path)
        header = cache_trace_reader._header

        sample_df = read_csv(cache_trace_path, names=header)
        print(sample_df)

        mean_read_iat = sample_df[sample_df['op'] == 'r']['iat'].mean()
        print(mean_read_iat, full_cache_feature_dict['mean_read_iat'])

        

    else:
        print("Did not find output {}.".format(output_path))



if __name__ == "__main__":
    main()