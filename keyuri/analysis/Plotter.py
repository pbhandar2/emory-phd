from pathlib import Path 
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from pandas import DataFrame

FONT_SIZE = 30
FIG_SIZE = [28, 10]


def is_read(feature_name):
    return '_r_' in feature_name or 'read' in feature_name


def get_diff_df(perf_df, cur_feature):
    diff_arr = []
    it_arr = []
    for group, df in perf_df.groupby(by=["workload", "rate", "bits", "seed"]):
        df = df.sort_values(by=["it"])
        diff_df = df[cur_feature].diff()
        diff_df.iloc[0] = df.iloc[0][cur_feature]
        diff_arr.extend(diff_df.to_list())
        it_arr.extend(df["it"].to_list())
    assert len(diff_arr) == len(it_arr)
    return diff_arr, it_arr


def scatter_plot_pp_perf(
        pp_err_df: DataFrame, 
        err_group_list: list, 
        output_path: Path,
        font_size: int = FONT_SIZE,
        fig_size: list = FIG_SIZE
):
    feature_to_title_map = {
        'cur_mean_read_size-cur_mean_write_size': "Size",
        'cur_mean_read_iat-cur_mean_write_iat': "Interarrival Time",
        'misalignment_per_read-misalignment_per_write': "Misalignment",
        'mean_r_per-mean_w_per': "Hit Rate",
        'write_ratio': "Write Ratio",
        'mean': "Mean"
    }

    plt.rcParams.update({'font.size': font_size})
    fig, axs = plt.subplots(figsize=fig_size, nrows=len(err_group_list))

    for pair_index, err_pair in enumerate(err_group_list):
        y_val_arr = []
        x_val_arr = []
        legend_arr = []
        feature_set_key = '-'.join(err_pair)
        for feature_name in err_pair:
            if is_read(feature_name):
                legend_arr.append("Read")
            else:
                legend_arr.append("Write")
        
            diff_arr, it_arr = get_diff_df(pp_err_df, feature_name)
            y_val_arr.append(diff_arr)
            x_val_arr.append(it_arr)
        
        if len(legend_arr) <= 1:
            legend_arr = []

        axs[pair_index].set_title(feature_to_title_map[feature_set_key])
        scatter_plot(axs[pair_index], x_val_arr, y_val_arr, legend_arr)

        if pair_index < len(err_group_list) - 1:
            axs[pair_index].set_xticks([])
        else:
            axs[pair_index].set_xlabel('Iteration', fontsize=40)
    
    fig.text(0.01, 0.5, 'Delta Error (%)', va='center', rotation='vertical', fontsize=40)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)


def scatter_plot(
        ax: Axes,
        x_val_arr: list,
        y_val_arr: list, 
        legend_arr: list
) -> None:
    marker_arr = ['P', 'o']
    color_arr = ['g', 'b']

    index = 0 
    for x_arr, y_arr in zip(x_val_arr, y_val_arr):
        if not legend_arr:
            ax.scatter(x_arr, y_arr, marker=marker_arr[index], s=275, alpha=0.5, color=color_arr[index])
        else:
            ax.scatter(x_arr, y_arr, marker=marker_arr[index], s=275, alpha=0.5, color=color_arr[index], label=legend_arr[index])
        index += 1 
    
    if legend_arr:
        ax.legend()
    
    ax.axhline(0, linestyle='--', color='red')