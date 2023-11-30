from json import load 
from argparse import ArgumentParser

from cydonia.blksample.BrutePP import BrutePP
from keyuri.config.BaseConfig import BaseConfig


ALGO_NAME_BASE = "brute"


def main():
    parser = ArgumentParser(description="Post process sample block traces using brute force.")
    parser.add_argument("workload_name", type=str, help="The name of the workload.")
    parser.add_argument("rate", type=int, help="The rate of sampling.")
    parser.add_argument("seed", type=int, help="Random seed.")
    parser.add_argument("bits", type=int, help="Number of lower order bits of addresses that are ignored in the sample.")
    parser.add_argument("algo_bits", type=int, help="Number of lower order bits of addresses that are ignored by the algorithm.")
    parser.add_argument("--sample_type", type=str, default="iat", help="The type of sampling technique used.")
    parser.add_argument("--workload_type", type=str, default="cp", help="The type of workload.")
    parser.add_argument("--break_flag", action='store_true', help="If set to true, remove the first block you find that reduces error (default). Else, evaluate all blocks.")
    args = parser.parse_args()

    config = BaseConfig()
    algo_name = ALGO_NAME_BASE
    if args.break_flag:
        algo_name = "{}-break".format(ALGO_NAME_BASE)

    full_trace_workload_feature_file_path = config.get_cache_features_path(args.workload_name)
    with full_trace_workload_feature_file_path.open("r") as workload_feature_file_handle:
        full_trace_workload_feature_dict = load(workload_feature_file_handle)
    
    sample_cache_trace_path = config.get_sample_cache_trace_path(args.sample_type, args.workload_name, args.rate, args.bits, args.seed)
    brute_algo_output_file_path = config.get_algo_output_file_path(algo_name, args.sample_type, args.workload_name, args.rate, args.bits, args.seed, args.algo_bits)
    brute_algo_output_file_path.parent.mkdir(exist_ok=True, parents=True)

    print("Running brute force post processing for sample cache trace {} with algorithm bits {} in output file {}".format(sample_cache_trace_path,
                                                                                                                            args.algo_bits,
                                                                                                                            brute_algo_output_file_path))
    
    brute_pp = BrutePP(sample_cache_trace_path, full_trace_workload_feature_dict, brute_algo_output_file_path, args.algo_bits, args.break_flag)
    brute_pp.brute()


if __name__ == "__main__":
    main()