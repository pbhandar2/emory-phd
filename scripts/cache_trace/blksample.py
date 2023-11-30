"""
Example:
    python3 blksample.py w11 -r 1 -b 0 -s 42 -ab 0 
"""


from argparse import ArgumentParser 

from keyuri.config.BaseConfig import BaseConfig
from cydonia.blksample.lib import blksample, get_workload_feature_dict_from_block_trace
from cydonia.profiler.BlockAccessFeatureMap import BlockAccessFeatureMap


def main():
    parser = ArgumentParser(description="Post process sample block traces.")
    parser.add_argument("workload_name", 
                            type=str, 
                            help="The name of the workload.")

    parser.add_argument("-r",
                            "--rate", 
                            type=int, 
                            help="Sampling rate in percentage.")

    parser.add_argument("-s",
                            "--seed", 
                            type=int, 
                            help="Random seed of the sample.")

    parser.add_argument("-b",
                            "--bits", 
                            type=int, 
                            help="Number of lower order bits of addresses ignored.")
    

    parser.add_argument("-ab",
                            "--algobits", 
                            type=int, 
                            help="Number of lower order bits of addresses ignored in the algorithm.")
    

    parser.add_argument("--sample_type", 
                            type=str, 
                            default="iat", 
                            help="The sample type to be evaluated.")
    
    parser.add_argument("--workload_type", 
                            type=str, 
                            default="cp", 
                            help="The type of workload.")

    args = parser.parse_args()

    config = BaseConfig()
    block_trace_path = config.get_block_trace_path(args.workload_name)
    sample_block_trace_path = config.get_sample_block_trace_path(args.sample_type, args.workload_name, args.rate, args.bits, args.seed)
    per_iteration_output_path = config.get_per_iteration_output_file_path(args.sample_type, args.workload_name, args.rate, args.bits, args.seed, args.algobits)

    access_map = BlockAccessFeatureMap()
    access_map.load(sample_block_trace_path, args.algobits)

    per_iteration_output_path.parent.mkdir(exist_ok=True, parents=True)
    full_workload_feature_dict = get_workload_feature_dict_from_block_trace(block_trace_path)
    sample_workload_feature_dict = get_workload_feature_dict_from_block_trace(sample_block_trace_path)

    blksample(access_map, full_workload_feature_dict, sample_workload_feature_dict, per_iteration_output_path)
    

if __name__ == "__main__":
    main()