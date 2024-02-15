from argparse import ArgumentParser
from pandas import DataFrame

from keyuri.config.BaseConfig import BaseConfig


def main():
    parser = ArgumentParser(description="Find missing access feature files for samples and create them.")
    parser.add_argument("--rate", "-r",
                            default=0.1, 
                            type=float, 
                            help="Sampling rate of samples for which we look for access files.")
    parser.add_argument("--seed", "-s", 
                            default=42, 
                            type=int, 
                            help="Random seed of samples for which we look for access files.")
    parser.add_argument("--bits", "-b", 
                            default=4, 
                            type=int, 
                            help="Bits ignored in samples for which we look for access files.")
    args = parser.parse_args()

    sample_set_name = "basic"
    config = BaseConfig()
    workload_count = 107
    entry_arr = []
    for workload_num in range(1, workload_count):
        workload_name = "w{}".format(workload_num) if workload_num >= 10 else "w0{}".format(workload_num)

        sample_file_path = config.get_sample_cache_trace_path(sample_set_name, 
                                                              workload_name, 
                                                              int(100*args.rate), 
                                                              args.bits, 
                                                              args.seed)

        sample_feature_file_path = config.get_sample_cache_features_path(sample_set_name,
                                                                         workload_name,
                                                                         int(100*args.rate),
                                                                         args.bits,
                                                                         args.seed)
        
        hit_rate_error_file_path = config.get_hit_rate_error_file_path(sample_set_name, 
                                                                        workload_name, 
                                                                        int(100*args.rate),
                                                                        args.bits, 
                                                                        args.seed)

        access_file_path = config.get_sample_access_feature_file_path(sample_set_name,
                                                                      workload_name,
                                                                      int(100*args.rate),
                                                                      args.bits,
                                                                      args.seed)

        
        if not sample_file_path.exists():
            print("sample file {} does not exist.".format(sample_file_path))
        
        workload_entry = {
            "name": workload_name,
            "sample": sample_file_path.exists(),
            "features": sample_feature_file_path.exists(),
            "hr_err": hit_rate_error_file_path.exists(), 
            "access": access_file_path.exists()
        }
        entry_arr.append(workload_entry)
    
    data_df = DataFrame(entry_arr)
    print("Workloads missing samples.")
    print(data_df[data_df["sample"]==False])
    print("Workloads with samples but no features.")
    print(data_df[(data_df['sample']==True) & (data_df['features']==False)])
    print("Workloads with sample features but not hit rate error files.")
    print(data_df[(data_df['features']==True) & (data_df['hr_err']==False)])
    print("Workloads with samples but no access features.")
    print(data_df[(data_df['sample']==True) & (data_df['access']==False)])



if __name__ == "__main__":
    main()