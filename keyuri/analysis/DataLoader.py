from pathlib import Path 
from pandas import DataFrame, read_csv

from cydonia.profiler.WorkloadStats import WorkloadStats
from keyuri.config.BaseConfig import BaseConfig
from keyuri.config.BaseConfig import get_all_cp_workloads



def get_sample_workload_error_df(
        sample_set_name: str = "basic",
        dir_config: BaseConfig = BaseConfig()
) -> DataFrame:
    """ Get a DataFrame of percent error in sample traces. This is a cheap operation so we can rerun it everytime. """
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



def create_sample_hr_df_file(output_file_path):
    """ Create a file with details of hr error of samples. """
    if output_file_path.exists():
        print("Output file {} already exists.".format(output_file_path))
        return 

    sample_hr_error_df = get_sample_hr_error_df()
    sample_hr_error_df.to_csv(output_file_path, index=False)


def get_sample_hr_error_df(
        current_sample_hr_file: Path = None, 
        sample_set_name: str = "basic",
        dir_config: BaseConfig = BaseConfig()
) -> DataFrame:
    current_sample_hr_df = read_csv(current_sample_hr_file) if current_sample_hr_file is not None else None 
    error_dict_arr = []
    for workload_name in get_all_cp_workloads():
        print("Workload completed!", workload_name)
        for hit_rate_error_file in dir_config.get_hit_rate_error_data_dir_path(sample_set_name, workload_name).iterdir():
            rate, bits, seed = dir_config.get_sample_file_info(hit_rate_error_file)
            # if current_sample_hr_df:
            #     row = current_sample_hr_df[(current_sample_hr_df[])]

            # load hit rate error df 
            hit_rate_df = read_csv(hit_rate_error_file)

            # get mean, p99 of hit rate error (read/write/overall)
            percent_diff_dict = {}
            percent_diff_dict["read_mean_error"] = hit_rate_df["percent_read_hr"].mean()
            percent_diff_dict["read_p99_error"] = hit_rate_df["percent_read_hr"].quantile(0.99)
            percent_diff_dict["write_mean_error"] = hit_rate_df["percent_write_hr"].mean()
            percent_diff_dict["write_p99_error"] = hit_rate_df["percent_write_hr"].quantile(0.99)
            percent_diff_dict["overall_mean_error"] = hit_rate_df["percent_overall_hr"].mean()
            percent_diff_dict["overall_p99_error"] = hit_rate_df["percent_overall_hr"].quantile(0.99)

            
            percent_diff_dict["rate"], percent_diff_dict["bits"], percent_diff_dict["seed"] = rate, bits, seed
            percent_diff_dict["workload"] = workload_name
            print(percent_diff_dict)
            error_dict_arr.append(percent_diff_dict)

    return DataFrame(error_dict_arr)
