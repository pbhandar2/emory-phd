from argparse import ArgumentParser
from json import load 
from pathlib import Path 
from pandas import DataFrame
import matplotlib.pyplot as plt

from keyuri.config.BaseConfig import BaseConfig


OUTPUT_PLOT_DIR = Path("./files/error_vs_rate/")
COMPARE_FEATURE_LIST = ["write_block_req_split", "write_cache_req_split", "read_size_avg", "write_size_avg", "iat_read_avg", "iat_write_avg"]
STYLE_DICT = {
    0: '-*r',
    1: '-^g',
    2: '-ob',
    4: '-dc'
}


def get_err_df(sample_type, cur_seed: int = 42):
    err_dict_list = []
    base_config = BaseConfig()
    cache_feature_file_path_list = list(base_config._cache_features_dir_path.iterdir())
    for cache_feature_file_path in cache_feature_file_path_list:
        try:
            with cache_feature_file_path.open("r") as file_handle:
                feature_dict = load(file_handle)
        except:
            continue 

        workload_name = cache_feature_file_path.stem 
        sample_cache_feature_file_list = base_config.get_all_sample_cache_features(sample_type, workload_name)
        for sample_cache_feature_file_path in sample_cache_feature_file_list:
            split_sample_file_name = sample_cache_feature_file_path.stem.split('_')
            rate, bits, seed = int(split_sample_file_name[0]), int(split_sample_file_name[1]), int(split_sample_file_name[2])
            if seed != cur_seed:
                continue
            try:
                with sample_cache_feature_file_path.open("r") as file_handle:
                    sample_feature_dict = load(file_handle)
            except:
                continue 

            error_dict = {}
            error_dict["rate"] = rate 
            error_dict["bits"] = bits 
            error_dict["seed"] = seed 
            error_dict["workload"] = workload_name
            for feature_name in COMPARE_FEATURE_LIST:
                percent_err = 100.0 * (feature_dict[feature_name] - sample_feature_dict[feature_name])/feature_dict[feature_name]
                error_dict[feature_name] = percent_err
            
            err_dict_list.append(error_dict)

    return DataFrame(err_dict_list)


def plot(group_df, output_dir):
    print("Plot at {}".format(output_dir))
    print(group_df)
    output_dir.mkdir(exist_ok=True, parents=True)
    for feature_name in COMPARE_FEATURE_LIST:
        plt.rcParams.update({'font.size': 22})
        fig, ax = plt.subplots(figsize=[14,10])
        line_dict = {}
        for num_bits, bits_df in group_df.groupby(by=["bits"]):
            sorted_df = bits_df.sort_values(by=['rate'])
            line_dict[num_bits] = [sorted_df['rate'].abs().to_list(), sorted_df[feature_name].abs().to_list()]
            ax.plot(sorted_df['rate'].abs().to_list(), sorted_df[feature_name].abs().to_list(), STYLE_DICT[num_bits], label=num_bits, markersize=20)
        
        ax.set_ylabel("Percent Error (%)")
        ax.set_xlabel("Sample Rate (%)")
        plt.legend()
        plt.savefig(output_dir.joinpath("{}.png".format(feature_name)))
        plt.close(fig)


def main():
    parser = ArgumentParser(description="Plot error vs rate for different features with multiple lines each representing number of lower-order bits ignored.")
    parser.add_argument("--sample_type", type=str, default="iat", help="The sample type.")
    args = parser.parse_args()

    err_df = get_err_df(args.sample_type)
    if len(err_df) == 0:
        print("No comparisons found!")
        return 
    
    for group_tuple, group_df in err_df.groupby(by=["workload"]):
        workload_name = str(group_tuple)
        output_path = OUTPUT_PLOT_DIR.joinpath("{}".format(workload_name))
        plot(group_df, output_path)


if __name__ == "__main__":
    main()