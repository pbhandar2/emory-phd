from argparse import ArgumentParser 
from pandas import read_csv, DataFrame
from json import load 
import numpy as np 

from keyuri.config.BaseConfig import BaseConfig


def get_feature_err_dict(full_cache_feature_file_path, sample_cache_feature_file_path):
    with full_cache_feature_file_path.open('r') as f:
        full_cache_feature_dict = load(f)

    with sample_cache_feature_file_path.open('r') as f:
        sample_cache_feature_dict = load(f)

    err_dict = {}
    for feature_key in full_cache_feature_dict:
        percent_err = 100*(full_cache_feature_dict[feature_key] - sample_cache_feature_dict[feature_key])/full_cache_feature_dict[feature_key]
        err_dict[feature_key] = percent_err
    return err_dict


def main():
    parser = ArgumentParser(description="Compile cache features.")
    parser.add_argument("-t", "--type", type=str, default="basic", help="The type of sample.")
    args = parser.parse_args()

    base_config = BaseConfig()
    feature_err_list = []
    for workload_num in range(1, 107):
        workload_name = "w{}".format(workload_num) if workload_num >= 10 else "w0{}".format(workload_num)
        full_cache_feature_file_path = base_config.get_cache_features_path(workload_name)
        if not full_cache_feature_file_path.exists():
            continue 
        
        sample_cache_feature_file_list = base_config.get_all_sample_cache_features(args.type, workload_name)
        print("Workload {}".format(workload_name))
        for sample_cache_feature_file_path in sample_cache_feature_file_list:
            if not sample_cache_feature_file_path.exists():
                continue 
            rate, bits, seed = base_config.get_sample_file_info(sample_cache_feature_file_path)
            hit_rate_error_file_path = base_config.get_hit_rate_error_file_path(args.type, workload_name, rate, bits, seed)
            feature_err_dict = get_feature_err_dict(full_cache_feature_file_path, sample_cache_feature_file_path)

            if hit_rate_error_file_path.exists():
                hr_df = read_csv(hit_rate_error_file_path)
                feature_err_dict["percent_overall_hr"] = float(hr_df["percent_overall_hr"].mean())
                feature_err_dict["percent_read_hr"] = float(hr_df["percent_read_hr"].mean())
                feature_err_dict["percent_write_hr"] = float(hr_df["percent_write_hr"].mean())
                mean_err = float(np.mean([abs(feature_err_dict[key]) for key in feature_err_dict]))
                feature_err_dict["mean"] = mean_err
                feature_err_dict["workload"] = workload_name 
                feature_err_dict["rate"], feature_err_dict["bits"], feature_err_dict["seed"] = rate, bits, seed 

                feature_err_list.append(feature_err_dict)
        
        print(DataFrame(feature_err_list))
        
    feature_df = DataFrame(feature_err_list)
    print(feature_df)
    print(feature_df.sort_values(by=["mean"]))
    feature_df.to_csv("./files/sample_features/features.csv", index=False)




if __name__ == "__main__":
    main()
