from importlib import metadata
from pathlib import Path 
from json import load, dumps
from argparse import ArgumentParser
from matplotlib.markers import MarkerStyle 
import matplotlib.pyplot as plt
from collections import OrderedDict

from keyuri.config.Config import GlobalConfig


def scatter_plot(
        x_arr: list, 
        y_arr: list, 
        label_arr: list, 
        x_label: str, 
        y_label: str, 
        output_path: str, 
        color_arr: list = ['darkorange', 'g', 'b', 'magenta', 'orange', 'cyan'], 
        marker_arr: list = ['X', 'o', 's', 'd', '8', '*', 'p']
) -> None:
    max_val = max(max(x_arr), max(y_arr))
    plt.rcParams.update({'font.size': 22})
    fig, ax = plt.subplots(figsize=[14,10])

    for x, y, label in zip(x_arr, y_arr, label_arr):
        label_set = set(label_arr)
        label_index = list(label_set).index(label)
        ax.scatter(x, 
                    y, 
                    s=125,
                    alpha=0.4, 
                    label=label, 
                    color=color_arr[label_index], marker=MarkerStyle(marker_arr[label_index]))
        
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_xlim(0, max_val*1.05)
    ax.set_ylim(-1.2, max_val*1.05)
    ax.axhline(0, linewidth=1, color='gray', linestyle="--")
    ax.axhline(10, linewidth=1, color='gray', linestyle="--")

    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = OrderedDict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys())
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)


def scatter_plot_post_processing_error(config: GlobalConfig):
    metadata_dir = config.postprocess_stat_dir_path

    post_processing_err_arr = []
    sample_err_arr, label_arr = [], []
    postprocess_size_arr, postprocess_sampling_rate = [], []
    
    for file_path in metadata_dir.glob("*/*/*/*/*"):
        with file_path.open("r") as file_handle:
            metadata_dict = load(file_handle)
        
        full_file_size = metadata_dict["full_file_size"]
        postprocess_file_size = metadata_dict["postprocess_file_size"]

        full_lba_count = metadata_dict["full_lba_count"]
        postprocess_lba_count = metadata_dict["postprocess_lba_count"]

        postprocess_size_arr.append(100*postprocess_file_size/full_file_size)
        postprocess_sampling_rate.append(100*postprocess_lba_count/full_lba_count)
        
        print(file_path)
        print(dumps(metadata_dict, indent=2))

        sample_err_arr.append(metadata_dict["sample_percent_err"]["mean"])
        post_processing_err_arr.append(metadata_dict["postprocess_percent_err"]["mean"])
        label_arr.append(int(metadata_dict["specified_rate"]))

    scatter_plot(postprocess_size_arr, postprocess_sampling_rate, label_arr, 
                    "BlkSample Sample Size (%)", "BlkSample Sampling Rate (%)", "./files/post_process_scatter/scatter_size_rate.pdf")
    
    scatter_plot(sample_err_arr, post_processing_err_arr, label_arr, 
                    "Sampling Only Error (%)", "Sampling + BlkSample Error (%)", "./files/post_process_scatter/scatter.pdf")


def main():
    global_config = GlobalConfig()
    parser = ArgumentParser(description="Create a scatter plot of mean error vs effective sampling rate with and without postprocessing.")
    args = parser.parse_args()
    scatter_plot_post_processing_error(global_config)


if __name__ == "__main__":
    main()