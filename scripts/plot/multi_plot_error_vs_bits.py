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
    parser.add_argument("--plot_dir", default=Path("./files/multi_error_vs_bits"), help="Path to directory with error vs bits plots.")
    args = parser.parse_args()

    if args.source_dir_path != global_config.source_dir_path:
        global_config.update_source_dir(args.source_dir_path) 

    print("Source dir: {}".format(global_config.source_dir_path))
    sample_metadata = SampleMetadata(args.workload_name, global_config=global_config)

    percent_diff_df = sample_metadata.load_percent_diff_df() 
    print(list(percent_diff_df.columns))
    #print(percent_diff_df['read_iat_avg'])
    plot_feature_arr = ["read_size_avg", 
                        "write_size_avg", 
                        "write_wss_split",
                        'iat_read_avg', 
                        'iat_write_avg', 
                        "write_block_req_split",
                        'write_cache_req_split',
                        'r_hr_20',
                        'w_hr_20']

    plot_feature_map = {
        "read_size_avg": "Mean Read Size",
        "write_size_avg": "Mean Write Size",
        "write_wss_split": "Write Working Set Size Ratio",
        "iat_read_avg": "Mean Read Inter-arrival Time",
        "iat_write_avg": "Mean Write Inter-arrival Time",
        "r_hr_20": "Read Hit Rate with size of 20% of WSS",
        "w_hr_20": "Write Hit Rate with size of 20% of WSS",
        "write_block_req_split": "Ratio of write requests",
        "write_cache_req_split": "Ratio of write block requests"
    }
    
    plot = Plot(row_count=3, col_count=3)
    df_column_arr = list(percent_diff_df.columns)
    for feature_index, feature_name in enumerate(plot_feature_arr):
        line_dict = defaultdict(list)

        if feature_name not in df_column_arr:
            continue 

        for (sample_rate, bit), sample_df in percent_diff_df.groupby(by=["rate", "bit"]):
            print(feature_name)
            best_row = sample_df[sample_df[feature_name]==sample_df[feature_name].min()].iloc[0]
            line_dict[int(sample_rate)].append([int(bit), float(best_row[feature_name])])

        x_arr, y_arr, legend_arr = [], [], []
        for sample_rate in sample_config.rate_arr:
            sorted_arr = sorted(line_dict[sample_rate], key=lambda k: k[0])
            bits_arr, error_arr = [_[0] for _ in sorted_arr], [_[1] for _ in sorted_arr]
            x_arr.append(bits_arr)
            y_arr.append(error_arr)
            legend_arr.append(int(sample_rate))
        
        plot.multi_line_plot(x_arr, y_arr, legend_arr, plot_index=feature_index)
        plot.set_title(plot_feature_map[feature_name], plot_index=feature_index)

    plot.set_legend()
    plot_path = args.plot_dir.joinpath("{}_{}.pdf".format(args.workload_type, args.workload_name))
    plot.savefig(plot_path)

    # line_dict = defaultdict(list)
    # for (sample_rate, bit), sample_df in percent_diff_df.groupby(by=["rate", "bit"]):
    #     best_row = sample_df[sample_df["mean_overall_error"]==sample_df["mean_overall_error"].min()].iloc[0]
    #     line_dict[int(sample_rate)].append([int(bit), float(best_row['mean_overall_error'])])
    
    # x_arr, y_arr, legend_arr = [], [], []
    # line_plot = Plot()
    # for sample_rate in sample_config.rate_arr:
    #     sorted_arr = sorted(line_dict[sample_rate], key=lambda k: k[0])
    #     bits_arr, error_arr = [_[0] for _ in sorted_arr], [_[1] for _ in sorted_arr]
    #     x_arr.append(bits_arr)
    #     y_arr.append(error_arr)
    #     legend_arr.append(int(sample_rate))
    
    # line_plot.multi_line_plot(x_arr, y_arr, legend_arr, "Lower Order Address Bits Ignored", "Mean Absolute Percent Error")

    # # handles, labels = line_plot.ax.get_legend_handles_labels()
    # # order = argsort(legend_arr)
    # # print(handles, labels, order, legend_arr)
    # # line_plot.ax.legend([handles[idx] for idx in order],[labels[idx] for idx in order])

    # plot_path = args.plot_dir.joinpath("{}_{}.pdf".format(args.workload_type, args.workload_name))
    # line_plot.savefig(plot_path)



if __name__ == "__main__":
    main()