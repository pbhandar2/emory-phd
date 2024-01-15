""" Plot feature error vs number of address bits ignored for a given workload and sample rate.
"""

from argparse import ArgumentParser
from pandas import DataFrame, read_csv 
from json import load
from pathlib import Path 

import matplotlib.pyplot as plt

from keyuri.config.BaseConfig import BaseConfig
from keyuri.tracker.sample import check_if_sample_error_set, get_sample_error_file_list

from cydonia.profiler.WorkloadStats import WorkloadStats


def subplot(ax: plt.Axes, post_process_output_df: DataFrame, workload_feature_dict: dict, op_type: str):
    size_arr = [workload_feature_dict["cur_mean_{}_size".format(op_type)]] + post_process_output_df["cur_mean_{}_size".format(op_type)].to_list()
    iat_arr = [workload_feature_dict["cur_mean_{}_iat".format(op_type)]] + post_process_output_df["cur_mean_{}_iat".format(op_type)].to_list()
    misalignment_arr = [workload_feature_dict["misalignment_per_{}".format(op_type)]] + post_process_output_df["misalignment_per_{}".format(op_type)].to_list()
    mean_arr = [workload_feature_dict["mean"]] + post_process_output_df["mean"].to_list()
    num_iter = len(post_process_output_df) + 1

    markevery_val=150

    ax.plot(range(num_iter), size_arr, 'b-*', markevery=markevery_val, markersize=20, alpha=0.5, label="Size")
    ax.plot(range(num_iter), iat_arr, 'g-^', markevery=markevery_val, markersize=20, alpha=0.5, label="IAT")
    ax.plot(range(num_iter), misalignment_arr, 'r-8', markevery=markevery_val, markersize=20, alpha=0.5, label="Misalignment")
    ax.plot(range(num_iter), mean_arr, 'y-s', markevery=markevery_val, markersize=20, alpha=0.5, label="Mean")
    



def plot(post_process_output_df: DataFrame, workload_feature_dict: dict, plot_path: Path):
    plt.rcParams.update({'font.size': 22})
    fig, axs = plt.subplots(2, figsize=[14,10])

    subplot(axs[0], post_process_output_df, workload_feature_dict, "read")
    subplot(axs[1], post_process_output_df, workload_feature_dict, "write")

    # subplot(axs[0], "read", data, hit_rate_data=hit_rate_data)
    axs[0].legend(loc="upper center", 
                  bbox_to_anchor=(0.5, 1.225),
                  ncol=4)
    # subplot(axs[1], "write", data, hit_rate_data=hit_rate_data)
    axs[1].set_xlabel("Number of iterations")

    fig.text(0.04, 0.5, 'Percent Error (%)', va='center', rotation='vertical')
    plot_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(plot_path)
    plt.close(fig)


def main():
    parser = ArgumentParser(description="Plot post processing output.")
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
    parser.add_argument("-b",
                            "--bits",
                            type=int,
                            help="Number of lower order bits of block keys ignored during sampling.")
    parser.add_argument("-ab",
                            "--abits",
                            type=int,
                            help="Number of lower order bits of block keys ignored during post processing.")
    parser.add_argument("-a",
                            "--algo",
                            type=str,
                            help="The algorithm type.")
    parser.add_argument("-o",
                            "--output_dir",
                            type=str,
                            default="./files/algo_output",
                            help="The output directory where plots are stored.")
    args = parser.parse_args()

    base_config = BaseConfig()

    plot_path = Path(args.output_dir).joinpath("{}/{}/{}/{}_{}_{}_{}.png".format(base_config.get_compound_workload_set_name(args.type), 
                                                                        args.workload, 
                                                                        args.algo,
                                                                        int(100 * args.rate),
                                                                        args.bits,
                                                                        args.seed,
                                                                        args.abits))
    
    
    output_path = base_config.get_sample_post_process_output_file_path(args.type,
                                                                        args.workload,
                                                                        args.algo,
                                                                        args.abits,
                                                                        int(100 * args.rate),
                                                                        args.bits,
                                                                        args.seed)

    if output_path.exists():
        print("Found output {}.".format(output_path))
        output_df = read_csv(output_path)
        print(output_df)

        sample_error_file_path = base_config.get_sample_block_error_file_path(args.type,
                                                                                args.workload,
                                                                                int(100 * args.rate),
                                                                                args.bits,
                                                                                args.seed)
        
        with sample_error_file_path.open("r") as handle:
            sample_error_dict = load(handle)

        print(sample_error_dict)
        plot(output_df, sample_error_dict, plot_path)
    else:
        print("Did not find output {}.".format(output_path))



if __name__ == "__main__":
    main()