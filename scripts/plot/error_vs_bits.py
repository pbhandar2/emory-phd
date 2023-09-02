from pathlib import Path 
from itertools import product 
from argparse import ArgumentParser
from json import load as json_load 
from numpy import argsort 
from collections import defaultdict

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig
from keyuri.analysis.SampleMetadata import SampleMetadata
from keyuri.analysis.Plot import Plot


def main():
    global_config, sample_config = GlobalConfig(), SampleExperimentConfig()

    parser = ArgumentParser(description="Plot error of best sample among different seeds across bits ignored.")
    parser.add_argument("workload_name", help="Name of the workload")
    parser.add_argument("--workload_type", default="cp", help="Type of the workload")
    parser.add_argument("--source_dir_path", type=Path, default=global_config.source_dir_path, help="Source directory of all data.")
    parser.add_argument("--plot_dir", default=Path("./files/error_vs_bits"), help="Path to directory with error vs bits plots.")
    args = parser.parse_args()

    if args.source_dir_path != global_config.source_dir_path:
        global_config.update_source_dir(args.source_dir_path) 

    print("Source dir: {}".format(global_config.source_dir_path))
    sample_metadata = SampleMetadata(args.workload_name, global_config=global_config)

    percent_diff_df = sample_metadata.load_percent_diff_df()
    line_dict = defaultdict(list)
    for (sample_rate, bit), sample_df in percent_diff_df.groupby(by=["rate", "bit"]):
        best_row = sample_df[sample_df["mean_overall_error"]==sample_df["mean_overall_error"].min()].iloc[0]
        line_dict[int(sample_rate)].append([int(bit), float(best_row['mean_overall_error'])])
    
    x_arr, y_arr, legend_arr = [], [], []
    line_plot = Plot()
    for sample_rate in sample_config.rate_arr:
        sorted_arr = sorted(line_dict[sample_rate], key=lambda k: k[0])
        bits_arr, error_arr = [_[0] for _ in sorted_arr], [_[1] for _ in sorted_arr]
        x_arr.append(bits_arr)
        y_arr.append(error_arr)
        legend_arr.append(int(sample_rate))
    
    line_plot.multi_line_plot(x_arr, y_arr, legend_arr, "Lower Order Address Bits Ignored", "Mean Absolute Percent Error")

    # handles, labels = line_plot.ax.get_legend_handles_labels()
    # order = argsort(legend_arr)
    # print(handles, labels, order, legend_arr)
    # line_plot.ax.legend([handles[idx] for idx in order],[labels[idx] for idx in order])

    plot_path = args.plot_dir.joinpath("{}_{}.pdf".format(args.workload_type, args.workload_name))
    line_plot.savefig(plot_path)



if __name__ == "__main__":
    main()