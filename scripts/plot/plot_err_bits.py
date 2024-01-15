""" Plot feature error vs number of address bits ignored for a given workload and sample rate. """

from argparse import ArgumentParser
from pandas import DataFrame, read_csv 
from pathlib import Path 
from json import load

import matplotlib.pyplot as plt

from keyuri.config.BaseConfig import BaseConfig
from keyuri.tracker.sample import check_if_sample_error_set, get_sample_error_file_list, check_if_hit_rate_error_set, get_sample_hit_rate_error_file_list


def get_size_feature_name(op_type: str):
    return "cur_mean_{}_size".format(op_type)


def get_iat_feature_name(op_type: str):
    return 


def subplot(ax: plt.Axes, op_type: str, data: DataFrame, hit_rate_data: DataFrame = None):
    size_arr = data["cur_mean_{}_size".format(op_type)]
    iat_arr = data["cur_mean_{}_iat".format(op_type)]
    misalignment_arr = data["misalignment_per_{}".format(op_type)]
    bits_arr = data["bits"]

    ax.plot(bits_arr, size_arr, 'b-*', markersize=20, alpha=0.5, label="Size")
    ax.plot(bits_arr, iat_arr, 'g-^', markersize=20, alpha=0.5, label="IAT")
    ax.plot(bits_arr, misalignment_arr, 'r-8', markersize=20, alpha=0.5, label="Misalignment")

    if hit_rate_data is not None:
        hit_rate_arr = hit_rate_data["{}_hr".format(op_type)]
        ax.plot(bits_arr, hit_rate_arr, 'y-s', markersize=20, alpha=0.5, label="MRC")
    
    ax.set_xticks(bits_arr)


def plot(data: DataFrame, plot_path: Path, hit_rate_data: DataFrame = None):
    plt.rcParams.update({'font.size': 22})
    fig, axs = plt.subplots(2, figsize=[14,10])

    subplot(axs[0], "read", data, hit_rate_data=hit_rate_data)
    axs[0].legend(loc="upper center", 
                  bbox_to_anchor=(0.5, 1.225),
                  ncol=4)
    subplot(axs[1], "write", data, hit_rate_data=hit_rate_data)
    axs[1].set_xlabel("Number of lower address bits ignored")

    fig.text(0.04, 0.5, 'Percent Error (%)', va='center', rotation='vertical')
    plot_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(plot_path)
    plt.close(fig)


def load_data(
        sample_set_name: str,
        workload_name: str,
        sample_rate_arr: list,
        random_seed_arr: list,
        base_config: BaseConfig = BaseConfig()
) -> DataFrame:
    sample_error_file_list = get_sample_error_file_list(sample_set_name,
                                                                    workload_name,
                                                                    random_seed_arr=random_seed_arr,
                                                                    sampling_rate_arr=sample_rate_arr,
                                                                    base_config=base_config)
    
    data_list = []
    for sample_error_file_path in sample_error_file_list:
        data_entry = {}
        rate, lower_addr_bits_ignored, seed = base_config.get_sample_file_info(sample_error_file_path)
        data_entry["rate"], data_entry["bits"], data_entry["seed"] = rate, lower_addr_bits_ignored, seed 

        with sample_error_file_path.open("r") as cache_feature_file_handle:
            sample_feature_dict = load(cache_feature_file_handle)
        data_entry.update(sample_feature_dict)
        data_list.append(data_entry)

    return DataFrame(data_list).sort_values(by="bits")


def load_hit_rate_data(
        sample_set_name: str,
        workload_name: str,
        sample_rate_arr: list,
        random_seed_arr: list,
        base_config: BaseConfig = BaseConfig()
) -> DataFrame:
    sample_error_file_list = get_sample_hit_rate_error_file_list(sample_set_name,
                                                                    workload_name,
                                                                    random_seed_arr=random_seed_arr,
                                                                    sampling_rate_arr=sample_rate_arr,
                                                                    base_config=base_config)
    
    data_list = []
    for sample_error_file_path in sample_error_file_list:
        data_entry = {}
        rate, lower_addr_bits_ignored, seed = base_config.get_sample_file_info(sample_error_file_path)
        data_entry["rate"], data_entry["bits"], data_entry["seed"] = rate, lower_addr_bits_ignored, seed 
        hit_rate_error_df = read_csv(sample_error_file_path)
        data_entry["read_hr"] = float(hit_rate_error_df["percent_read_hr"].mean())
        data_entry["write_hr"] = float(hit_rate_error_df["percent_write_hr"].mean())
        data_list.append(data_entry)

    return DataFrame(data_list).sort_values(by="bits")



def main():
    parser = ArgumentParser(description="Plot workload feature error against bits.")
    parser.add_argument("-w", 
                            "--workload", 
                            type=str, 
                            help="Name of workload.")
    parser.add_argument("-t",
                            "--type",
                            type=str,
                            help="The type of sample.")
    parser.add_argument("-r",
                            "--rate",
                            type=float,
                            help="Sampling Rate.")
    parser.add_argument("-s",
                            "--seed",
                            type=int,
                            help="Random seed.")
    parser.add_argument("-o",
                            "--output_dir",
                            type=str,
                            default="./files/err_bits",
                            help="The output directory where plots are stored.")
    args = parser.parse_args()


    base_config = BaseConfig()
    random_seed_arr = [args.seed]
    sample_rate_arr = [args.rate]
    plot_path = Path(args.output_dir).joinpath("{}/{}/{}_{}.png".format(base_config.get_compound_workload_set_name(args.type), args.workload, args.seed, int(100 * args.rate)))
    sample_cache_feature_file_set = check_if_sample_error_set(args.type,
                                                                args.workload,
                                                                random_seed_arr=random_seed_arr,
                                                                sampling_rate_arr=sample_rate_arr,
                                                                base_config=base_config)
    
    if not sample_cache_feature_file_set:
        print("Sample cache set not complete.")
        return 

    print("Generating plot at {}.".format(plot_path))
    plot_data = load_data(args.type,
                            args.workload,
                            sample_rate_arr,
                            random_seed_arr)
    print(plot_data.to_string())

    sample_cache_feature_file_set = check_if_hit_rate_error_set(args.type,
                                                                args.workload,
                                                                random_seed_arr=random_seed_arr,
                                                                sampling_rate_arr=sample_rate_arr,
                                                                base_config=base_config)
    
    if not sample_cache_feature_file_set:
        print("Hit rate error set does not exist.")
    else:
        print("Hit rate error set exists.")
        hit_rate_data = load_hit_rate_data(args.type,
                                           args.workload,
                                           sample_rate_arr,
                                           random_seed_arr)
        print(hit_rate_data)

        plot(plot_data, plot_path, hit_rate_data=hit_rate_data)


if __name__ == "__main__":
    main()