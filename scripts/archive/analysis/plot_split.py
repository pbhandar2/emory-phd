from argparse import ArgumentParser
from pandas import DataFrame 
from json import load 

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


def get_sample_split_df(
        workload_name: str, 
        workload_type: str = "cp",
        sample_type: str = "iat",
        global_config: GlobalConfig = GlobalConfig()
) -> DataFrame:
    sample_feature_arr = []
    sample_trace_dir = global_config.sample_block_feature_dir_path.joinpath(sample_type, workload_type, workload_name)
    for sample_block_feature_file_path in sample_trace_dir.iterdir():
        with sample_block_feature_file_path.open("r") as sample_block_feature_handle:
            sample_feature_dict = load(sample_block_feature_handle)
            sample_file_name = sample_block_feature_file_path.stem 
            rate, bits, seed = sample_file_name.split("_")
            sample_feature_dict['rate'] = rate 
            sample_feature_dict['bits'] = bits
            sample_feature_dict['seed'] = seed
            sample_feature_arr.append(sample_feature_dict)
    return DataFrame(sample_feature_arr)



def main():
    parser = ArgumentParser("Plot feature while varying bits.")
    parser.add_argument("workload_name", help="Name of the workload")
    parser.add_argument("--workload_type", default="cp", help="Type of the workload")
    args = parser.parse_args()

    global_config = GlobalConfig()
    sample_split_df = get_sample_split_df(args.workload_name)
    
    block_feature_file_path = global_config.get_block_feature_file_path(args.workload_type, args.workload_name)
    with block_feature_file_path.open("r") as block_feature_file_handle:
        block_feature_dict = load(block_feature_file_handle)
        
    
    rd_hist_file_path = global_config.get_rd_hist_file_path(args.workload_type, args.workload_name)
    
    sample_split_df['mean_read_size_per_error'] = 100*(block_feature_dict["read_size_avg"] - sample_split_df["read_size_avg"])/block_feature_dict["read_size_avg"]
    sample_split_df['mean_write_size_per_error'] = 100*(block_feature_dict["write_size_avg"] - sample_split_df["write_size_avg"])/block_feature_dict["write_size_avg"]
    sample_split_df['mean_iat_per_error'] = 100*(block_feature_dict["iat_avg"] - sample_split_df["iat_avg"])/block_feature_dict["iat_avg"]

    for group_index, group_df in sample_split_df.groupby(by=['bits', 'rate']):
        print(group_index)
        print(group_df[['rate', 'bits', 'seed', 'mean_read_size_per_error', 'mean_write_size_per_error', 'mean_iat_per_error']])

if __name__ == "__main__":
    main()
