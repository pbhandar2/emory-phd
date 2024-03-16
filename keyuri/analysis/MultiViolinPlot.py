import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from pathlib import Path 
from numpy import mean, std 
from collections import OrderedDict

from keyuri.analysis.DataLoader import get_sample_workload_error_df

DEFAULT_FONT_SIZE = 37
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

plt.rcParams.update({'font.size': DEFAULT_FONT_SIZE})


def load_sample_error_data(feature_name: str):
    sample_data = {}
    sample_error_df = get_sample_workload_error_df()
    for sample_rate, group_df in sample_error_df.groupby(by=["rate"]):
        if sample_rate not in sample_data:
            sample_data[sample_rate] = {}
        for bits, sub_group_df in group_df.groupby(by=["bits"]):
            if bits not in sample_data[sample_rate]:
                sample_data[sample_rate][bits] = []
            
            sample_data[sample_rate][bits].extend(sub_group_df[feature_name].to_list())
    return sample_data 


def plot_multi_level_bar_plot(
        output_path: Path,
        feature_name_map: dict = DEFAULT_FEATURE_MAP
) -> None:
    fig, ax = plt.subplots(figsize=[28,40], nrows=len(feature_name_map.values()), ncols=1)

    for feature_index, feature_name in enumerate(feature_name_map):
        cur_ax = ax[feature_index]
        data_dict = load_sample_error_data(feature_name)
        if feature_index < (len(feature_name_map.keys()) - 1):
            plot_multi_bar(data_dict, cur_ax, title_str=feature_name_map[feature_name], no_x_axis=True)
            cur_ax.set_xticks([])
        else:
            plot_multi_bar(data_dict, cur_ax, title_str=feature_name_map[feature_name])

    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = OrderedDict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)


def plot_multi_bar(
        data_dict: dict,
        ax: Axes,
        title_str: str = '',
        sampling_rate_arr: list = [5, 10, 20, 40, 80],
        num_lower_addr_bits_ignored_arr: list = [0, 1, 2, 4],
        no_x_axis: bool = False 
) -> None:
    if title_str:
        ax.set_title(title_str, fontsize=30)

    num_lower_addr_bits_ignored_count = len(num_lower_addr_bits_ignored_arr)

    x_axis_bits_tick_index = []
    x_axis_bits_tick_values = []
    x_axis_rate_tick_index = []
    x_axis_rate_tick_values = []

    cur_x_value = 1 
    for rate_index, sample_rate in enumerate(sampling_rate_arr):
        for bits in num_lower_addr_bits_ignored_arr:
            ax.boxplot([data_dict[sample_rate][bits]], positions=[cur_x_value], showfliers=False, widths = 0.8)
            x_axis_bits_tick_index.append(cur_x_value)
            x_axis_bits_tick_values.append(bits)
            cur_x_value += 1 


        x_axis_rate_tick_index.append(cur_x_value - (num_lower_addr_bits_ignored_count+1)/2.0)
        x_axis_rate_tick_values.append(sample_rate)
        cur_x_value += 1
    
    if not no_x_axis:
        ax.set_xticks(x_axis_bits_tick_index, x_axis_bits_tick_values, fontsize=25)
        sec = ax.secondary_xaxis(location="bottom")
        sec.set_xticks(x_axis_rate_tick_index, labels=["\n{}".format(_) for _ in x_axis_rate_tick_values], fontsize=30)
        ax.set_xlabel("\nSampling Rate")



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