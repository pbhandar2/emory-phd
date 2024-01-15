from argparse import ArgumentParser
from json import load, dump, dumps 
from pandas import DataFrame

from cydonia.profiler.BAFM import BAFM
from cydonia.profiler.WorkloadStats import WorkloadStats

from keyuri.config.BaseConfig import BaseConfig
from keyuri.config.ExperimentConfig import ExperimentConfig


def main():
    parser = ArgumentParser(description="Compute sample error.")
    parser.add_argument("workload_name", type=str, help="Name of workload.")
    parser.add_argument("sample_set_name", type=str, help="The name of sample set.")
    parser.add_argument("lower_addr_bits_ignored", type=int, help="Lower address bits ignored.")
    parser.add_argument("random_seed", type=int, help="Random seed.")
    args = parser.parse_args()

    base_config = BaseConfig()
    experiment_config = ExperimentConfig()
    full_cache_feature_set_flag, sample_cache_feature_file_list = experiment_config.get_sample_cache_feature_set(args.sample_set_name,
                                                                                                                    args.workload_name,
                                                                                                                    args.lower_addr_bits_ignored,
                                                                                                                    args.random_seed)
    full_cache_feature_file_path = base_config.get_cache_features_path(args.workload_name)
    if not full_cache_feature_file_path.exists():
        print("Full cache feature file {} does not exist.".format(full_cache_feature_file_path))
        return 
    
    with full_cache_feature_file_path.open("r") as handle:
        full_cache_feature_dict = load(handle)
        full_workload_stat = WorkloadStats()
        full_workload_stat.load_dict(full_cache_feature_dict)
    
    if full_cache_feature_set_flag:
        print("Full set exists.")
        err_dict_arr = []
        for sample_cache_feature_file_path in sample_cache_feature_file_list:
            with sample_cache_feature_file_path.open("r") as handle:
                sample_cache_feature_dict = load(handle)
                sample_workload_stat = WorkloadStats()
                sample_workload_stat.load_dict(sample_cache_feature_dict)

            rate, num_lower_addr_bits_ignored, seed = base_config.get_sample_file_info(sample_cache_feature_file_path)
            assert num_lower_addr_bits_ignored == args.lower_addr_bits_ignored and args.random_seed == seed, \
                "Seed and lower order bits should match here."
            
            sample_block_error_file_path = base_config.get_sample_block_error_file_path(args.sample_set_name,
                                                                                            args.workload_name,
                                                                                            rate,
                                                                                            num_lower_addr_bits_ignored,
                                                                                            seed)
            
            error_dict = BAFM.get_error_dict(full_workload_stat.get_workload_feature_dict(), 
                                                sample_workload_stat.get_workload_feature_dict())
            error_dict["bits"] = num_lower_addr_bits_ignored
            error_dict["rate"] = rate
            err_dict_arr.append(error_dict)
            sample_block_error_file_path.parent.mkdir(exist_ok=True, parents=True)
            with sample_block_error_file_path.open("w+") as handle:
                dump(error_dict, handle, indent=2)
        print(DataFrame(err_dict_arr))
    else:
        print("Full cache feature set does not exist!")



if __name__ == "__main__":
    main()