from argparse import ArgumentParser 
from pathlib import Path 
from pandas import read_csv, DataFrame, to_numeric

import matplotlib.pyplot as plt

from keyuri.config.BaseConfig import BaseConfig


def plot_req_ratio(plot_path: Path, data_df: DataFrame):
    data_df = data_df.sort_values(by=["rate"])

    print("Plot req ratio")
    print(data_df)
    
    plt.rcParams.update({'font.size': 37})
    fig, ax = plt.subplots(figsize=[14,10])
    ax.plot(data_df["rate"], data_df["req_ratio"], "-o", markersize=15)
    ax.set_xticks(data_df["rate"], data_df["rate"])
    #ax.axline((0, 0), slope=0.01, linestyle="--")
    ax.set_ylabel("Request Ratio")
    ax.set_xlabel("Sampling Rate (%)")
    plot_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(plot_path)
    plt.close(fig)


def plot_split_ratio(plot_path: Path, data_df: DataFrame):
    data_df = data_df.sort_values(by=["rate"])
    
    print("Plot split ratio")
    print(data_df)

    plt.rcParams.update({'font.size': 37})
    fig, ax = plt.subplots(figsize=[14,10])
    ax.plot(data_df["rate"], data_df["mean_sample_split"], "-o", markersize=15)
    ax.set_xticks(data_df["rate"], data_df["rate"])
    ax.set_ylim(1.0, None)
    ax.set_ylabel("Split Ratio")
    ax.set_xlabel("Sampling Rate (%)")
    plot_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(plot_path)
    plt.close(fig)


def main():
    parser = ArgumentParser(description="Plot sample features.")

    parser.add_argument("--workload",
                            "-w", 
                            type=str, 
                            help="Name of workload.")

    parser.add_argument("--type",
                            "-t", 
                            type=str, 
                            default="basic",
                            help="The sample type.")

    parser.add_argument("-o",
                            "--output_dir",
                            type=str,
                            default="./files/sample_features",
                            help="The output directory where plots are stored.")
    
    args = parser.parse_args()
    print("Plot sample features of workload {} and sample type {}.".format(args.workload, args.type))


    base_config = BaseConfig()

    sample_feature_path = base_config.get_sample_feature_file_path(args.type, args.workload)
    data_df = read_csv(sample_feature_path)

    plot_req_ratio_path = Path(args.output_dir).joinpath(args.type, args.workload, "req_ratio.pdf")
    plot_split_ratio_path = Path(args.output_dir).joinpath(args.type, args.workload, "split_ratio.pdf")



    plot_req_ratio(plot_req_ratio_path, data_df[data_df["bits"] == 0])
    plot_split_ratio(plot_split_ratio_path, data_df[data_df["bits"] == 0])


if __name__ == "__main__":
    main() 