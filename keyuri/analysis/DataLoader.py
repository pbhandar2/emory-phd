from pathlib import Path 
from pandas import DataFrame

from cydonia.profiler.WorkloadStats import WorkloadStats
from keyuri.config.BaseConfig import BaseConfig


def get_sample_workload_error_df(
        sample_set_name: str = "basic",
        dir_config: BaseConfig = BaseConfig()
) -> DataFrame:
    """ Get a DataFrame of percent error in sample traces. """
    error_dict_arr = []
    for full_workload_feature_file in dir_config.get_all_cache_features():
        # check size of workload feature file 
        if not full_workload_feature_file.stat().st_size:
            continue 

        workload_name = full_workload_feature_file.stem 
        full_workload_stat = WorkloadStats()
        full_workload_stat.load_file(full_workload_feature_file)
        for sample_workload_feature_file in dir_config.get_all_sample_cache_features(sample_set_name, workload_name):
            # check size of workload feature file 
            if not sample_workload_feature_file.stat().st_size:
                continue 
            
            sample_workload_stat = WorkloadStats()
            sample_workload_stat.load_file(sample_workload_feature_file)
            percent_diff_dict = full_workload_stat - sample_workload_stat

            rate, bits, seed = dir_config.get_sample_file_info(sample_workload_feature_file)
            percent_diff_dict["rate"], percent_diff_dict["bits"], percent_diff_dict["seed"] = rate, bits, seed
            percent_diff_dict["workload"] = workload_name
            error_dict_arr.append(percent_diff_dict)
    
    return DataFrame(error_dict_arr)