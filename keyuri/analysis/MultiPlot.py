import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from pathlib import Path 
from numpy import mean, std 
from collections import OrderedDict
from pandas import DataFrame

from keyuri.analysis.DataLoader import get_sample_workload_error_df, get_sample_hr_error_df

DEFAULT_FONT_SIZE = 30
DEFAULT_FIG_SIZE = [28, 10]
DEFAULT_FEATURE_MAP = {
    "cur_mean_read_size": "Read Size",
    "cur_mean_write_size": "Write Size",
    "cur_mean_read_iat": "Read IAT",
    "cur_mean_write_iat": "Write IAT",
    "misalignment_per_read": "Read Misalignment",
    "misalignment_per_write": "Write Misalignment",
    "write_ratio": "Write Ratio"
}
DEFAULT_HR_FEATURE_MAP = {
    "read_mean_error": "Mean Read Hit Rate",
    "read_p99_error": "P99 Read Hit Rate",
    "write_mean_error": "Mean Write Hit Rate",
    "write_p99_error": "P99 Write Hit Rate",
    "overall_mean_error": "Mean Overall Hit Rate",
    "overall_p99_error": "P99 Overall Hit Rate"
}
DEFAULT_REPLAY_PERF_MAP = {
    "bandwidth": "Bandwidth",
    "blockReadLatency_avg_ns": "BlockReadLatency\navg",
    "blockWriteLatency_avg_ns": "BlockWriteLatency\navg",
    "blockReadLatency_p99_ns": "BlockReadLatency\np99",
    "blockWriteLatency_p99_ns": "BlockWriteLatency\np99"
}

plt.rcParams.update({'font.size': DEFAULT_FONT_SIZE})


def get_data_from_error_df(sample_error_df: DataFrame, feature_name: str):
    sample_data = {}
    for sample_rate, group_df in sample_error_df.groupby(by=["rate"]):
        if sample_rate not in sample_data:
            sample_data[sample_rate] = {}
        for bits, sub_group_df in group_df.groupby(by=["bits"]):
            if bits not in sample_data[sample_rate]:
                sample_data[sample_rate][bits] = []
            
            sample_data[sample_rate][bits].extend(sub_group_df[feature_name].to_list())
    return sample_data 


def compute_sample_error_data(error_df: DataFrame, feature_map: dict) -> dict:
    sample_error_data = {}
    for feature_name in feature_map:
        sample_error_data[feature_name] = get_data_from_error_df(error_df, feature_name)
    return sample_error_data


def load_sample_error_data(feature_map: dict):
    sample_error_df = get_sample_workload_error_df()
    return compute_sample_error_data(sample_error_df, feature_map)


def load_sample_hr_error_data(feature_map: dict):
    sample_error_df = get_sample_hr_error_df()
    return compute_sample_error_data(sample_error_df, feature_map)


def plot_replay_bar_plot(
        data: dict,  
        output_path: Path,
        bits_arr: list = [0, 4]
) -> None:
    fig, ax = plt.subplots(figsize=[28,10])
    hatch_arr = ['//', 'o']

    cur_x_value = 1 
    xticks_index_arr = []
    xticks_val_arr = []
    for feature_key in data:
        xticks_index_arr.append(cur_x_value + 0.5)
        xticks_val_arr.append(DEFAULT_REPLAY_PERF_MAP[feature_key])
        for bit_index, bits in enumerate(bits_arr):
            mean_val = mean(data[feature_key][bits])
            std_val = std(data[feature_key][bits])
            ax.bar(cur_x_value, mean_val, color='white', edgecolor='black', hatch=hatch_arr[bit_index], label=bits_arr[bit_index])
            ax.text(cur_x_value, mean_val + 0.1, str(int(mean_val)), ha='center')
            #ax.errorbar(cur_x_value, mean_val, yerr=std_val)
            cur_x_value += 1
        
        cur_x_value += 1
    

        

    ax.set_xticks(xticks_index_arr, xticks_val_arr, fontsize=25)
    ax.set_xlabel("Performance Metric", fontsize=30)
    ax.set_ylabel("Mean Error (%)", fontsize=30)

    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = OrderedDict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)


def plot_multi_level_box_plot(
        output_path: Path,
        plot_type: str = "block_features"
) -> None:

    if plot_type == "block_features":
        feature_name_map = DEFAULT_FEATURE_MAP
        error_data = load_sample_error_data(feature_name_map)
    else:
        feature_name_map = DEFAULT_HR_FEATURE_MAP
        error_data = load_sample_hr_error_data(feature_name_map)

    fig, ax = plt.subplots(figsize=[32,40], nrows=len(feature_name_map.values()), ncols=1)
    for feature_index, feature_name in enumerate(feature_name_map):
        cur_ax = ax[feature_index]
        data_dict = error_data[feature_name]
        
        if feature_index < (len(feature_name_map.keys()) - 1):
            plot_multi_box(data_dict, cur_ax, title_str=feature_name_map[feature_name], no_x_axis=True)
            cur_ax.set_xticks([])
        else:
            plot_multi_box(data_dict, cur_ax, title_str=feature_name_map[feature_name])
        
        if feature_index == len(feature_name_map.keys())//2:
            cur_ax.set_ylabel("Error (%)", fontsize=55)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)


def plot_multi_box(
        data_dict: dict,
        ax: Axes,
        title_str: str = '',
        sampling_rate_arr: list = [5, 10, 20, 40, 80],
        num_lower_addr_bits_ignored_arr: list = [0, 1, 2, 4],
        no_x_axis: bool = False 
) -> None:
    if title_str:
        ax.set_title(title_str, fontsize=50)

    num_lower_addr_bits_ignored_count = len(num_lower_addr_bits_ignored_arr)

    x_axis_bits_tick_index = []
    x_axis_bits_tick_values = []
    x_axis_rate_tick_index = []
    x_axis_rate_tick_values = []

    cur_x_value = 1 
    for sample_rate in sampling_rate_arr:
        for bits in num_lower_addr_bits_ignored_arr:
            ax.boxplot([[abs(_) for _ in data_dict[sample_rate][bits]]], positions=[cur_x_value], showfliers=False, widths = 0.8)
            x_axis_bits_tick_index.append(cur_x_value)
            x_axis_bits_tick_values.append(bits)
            cur_x_value += 1 

        x_axis_rate_tick_index.append(cur_x_value - (num_lower_addr_bits_ignored_count+1)/2.0)
        x_axis_rate_tick_values.append(sample_rate)
        cur_x_value += 1
    
    if not no_x_axis:
        ax.set_xticks(x_axis_bits_tick_index, x_axis_bits_tick_values, fontsize=35)
        sec = ax.secondary_xaxis(location="bottom")
        sec.set_xticks(x_axis_rate_tick_index, labels=["\n{}".format(_) for _ in x_axis_rate_tick_values], fontsize=40)
        ax.set_xlabel("\nSampling Rate (%)", fontsize=55)


def plot_multi_bar(
        data_dict: dict,
        ax: Axes,
        title_str: str = '',
        sampling_rate_arr: list = [10, 20],
        num_lower_addr_bits_ignored_arr: list = [0, 4],
        no_x_axis: bool = False 
) -> None:
    if title_str:
        ax.set_title(title_str, fontsize=50)

    num_lower_addr_bits_ignored_count = len(num_lower_addr_bits_ignored_arr)

    x_axis_bits_tick_index = []
    x_axis_bits_tick_values = []
    x_axis_rate_tick_index = []
    x_axis_rate_tick_values = []

    cur_x_value = 1 
    for sample_rate in sampling_rate_arr:
        for bits in num_lower_addr_bits_ignored_arr:
            ax.boxplot([[abs(_) for _ in data_dict[sample_rate][bits]]], positions=[cur_x_value], showfliers=False, widths = 0.8)
            x_axis_bits_tick_index.append(cur_x_value)
            x_axis_bits_tick_values.append(bits)
            cur_x_value += 1 

        x_axis_rate_tick_index.append(cur_x_value - (num_lower_addr_bits_ignored_count+1)/2.0)
        x_axis_rate_tick_values.append(sample_rate)
        cur_x_value += 1
    
    if not no_x_axis:
        ax.set_xticks(x_axis_bits_tick_index, x_axis_bits_tick_values, fontsize=35)
        sec = ax.secondary_xaxis(location="bottom")
        sec.set_xticks(x_axis_rate_tick_index, labels=["\n{}".format(_) for _ in x_axis_rate_tick_values], fontsize=40)
        ax.set_xlabel("\nSampling Rate (%)", fontsize=55)

def multi_bar_feature_error_plot(
        data: dict, 
        output_file_path: Path,
        sampling_rate_arr: list = [1, 5, 10, 20, 40, 80],
        num_lower_addr_bits_ignored_arr: list = [0, 1, 2, 4]
) -> None:
    fig, ax = plt.subplots(figsize=[28,40])
    
    bar_pattern_dict = {
        0: '/',
        1: '*',
        2: 'O',
        4: 'x'
    }

    cur_x_value = 1 
    xticks_values = []
    for sample_rate in sampling_rate_arr:
        xticks_values.append(cur_x_value + 1.5)
        for bits in num_lower_addr_bits_ignored_arr:
            data_arr = [abs(_) for _ in data[sample_rate][bits]]
            mean_val, std_val = mean(data_arr), std(data_arr)
            ax.violin([mean_val], positions=[cur_x_value], hatch=bar_pattern_dict[bits])
            # ax.bar([cur_x_value], 
            #         [mean_val], 
            #         hatch=bar_pattern_dict[bits], 
            #         color='white', 
            #         edgecolor='black',
            #         label=bits)
            ax.errorbar([cur_x_value], [mean_val], yerr=std_val, fmt="o", color="r")
            cur_x_value += 1 
        cur_x_value += 1
    
    ax.set_xticks(xticks_values)
    ax.set_xticklabels(sampling_rate_arr)

    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = OrderedDict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())

    plt.savefig(output_file_path)
    plt.close(fig)