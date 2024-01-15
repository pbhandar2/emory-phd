from argparse import ArgumentParser 
from pathlib import Path 
from pandas import read_csv 

from keyuri.config.BaseConfig import BaseConfig

from cydonia.profiler.CacheTraceProfiler import basic_algo, get_workload_features_from_cache_trace


def run(
        cache_trace_path: Path,
        full_workload_feature_dict: dict,
        num_lower_order_bits_ignored: int 
) -> None:
    cache_trace_df = read_csv(cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])
    algo_df = basic_algo(cache_trace_df, full_workload_feature_dict, num_lower_order_bits_ignored)



def run_basic_algo(
        workload_name: str,
        rate: int,
        bits: int,
        seed: int,
        num_lower_order_bits_ignored: int,
        workload_type: str = "cp",
        sample_type: str = "iat"
) -> None:
    base_config = BaseConfig()
    sample_cache_trace_path = base_config.get_sample_cache_trace_path(sample_type, workload_name, rate, bits, seed)
    cache_trace_path = base_config.get_cache_trace_path(workload_name)
    cache_trace_df = read_csv(cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])
    full_workload_feature_dict = get_workload_features_from_cache_trace(cache_trace_df)
    run(sample_cache_trace_path, full_workload_feature_dict, num_lower_order_bits_ignored)



def main():
    parser = ArgumentParser(description="Run basic post-processing algorithm on a sample cache trace.")

    parser.add_argument("-w",
                            "--workload",
                            type=str,
                            help="Workload to post-process.")

    parser.add_argument("-r",
                            "--rate",
                            type=int,
                            help="Sampling Rate.")
    
    parser.add_argument("-b",
                            "--bits",
                            type=int,
                            help="Number of lower order bits of block keys ignored during sampling.")

    parser.add_argument("-s",
                            "--seed",
                            type=int,
                            help="Random seed.")
    
    parser.add_argument("-a",
                            "--algo",
                            type=int,
                            help="Number of lower order bits ignored by the algorithm.")
    
    args = parser.parse_args()

    run_basic_algo(args.workload, args.rate, args.bits, args.seed, args.algo)


if __name__ == "__main__":
    main()