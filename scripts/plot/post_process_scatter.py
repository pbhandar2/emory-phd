from importlib import metadata
from pathlib import Path 
from json import load, dumps
from argparse import ArgumentParser
from matplotlib.markers import MarkerStyle 
import matplotlib.pyplot as plt
from collections import OrderedDict
from pandas import DataFrame

from keyuri.config.Config import GlobalConfig


def get_info_from_metadata_file_path(metadata_file_path: Path) -> dict:
    info_dict = {}
    file_name = metadata_file_path.stem.split("_")
    info_dict["rate"], info_dict["bits"], info_dict["seed"] = int(file_name[0]), int(file_name[1]), int(file_name[2])
    info_dict["workload"] = metadata_file_path.parent.name 
    algo_name = metadata_file_path.parent.parent.parent.parent.name
    info_dict["algo_type"], info_dict["algo_param"] = algo_name.split("-")[0], algo_name.split("-")[1]
    return info_dict 


def load_data(
        config: GlobalConfig = GlobalConfig(),
        trace_type: str = "access"
) -> tuple:
    hrc_err_dir = config.postprocess_hrc_err_dir_path if trace_type == "cache" else config.postprocess_access_hrc_err_dir_path
    hrc_row_list = []
    for file_path in hrc_err_dir.glob("*/*/*/*/*"):
        with file_path.open("r") as file_handle:
            metadata_dict = load(file_handle)
        
        info_dict = get_info_from_metadata_file_path(file_path)
        info_dict["sample_mean_read_hr_err"] = metadata_dict["samp_1"]["mean"]
        info_dict["post_mean_read_hr_err"] = metadata_dict["post_1"]["mean"]
        info_dict["sample_mean_overall_hr_err"] = metadata_dict["samp_2"]["mean"]
        info_dict["post_mean_overall_hr_err"] = metadata_dict["post_2"]["mean"]
        info_dict["sample_mean_read_only_hr_err"] = metadata_dict["samp_3"]["mean"]
        info_dict["post_mean_read_only_hr_err"] = metadata_dict["post_3"]["mean"]
        info_dict["mean_read_hr_err"] = 100*(metadata_dict["samp_1"]["mean"] - metadata_dict["post_1"]["mean"])/metadata_dict["samp_1"]["mean"]
        info_dict["mean_overall_hr_err"] = 100*(metadata_dict["samp_2"]["mean"] - metadata_dict["post_2"]["mean"])/metadata_dict["samp_2"]["mean"]
        info_dict["mean_read_only_hr_err"] = 100*(metadata_dict["samp_3"]["mean"] - metadata_dict["post_3"]["mean"])/metadata_dict["samp_3"]["mean"]
        hrc_row_list.append(info_dict)

    overall_row_list = []
    overall_stat_dir = config.postprocess_stat_dir_path 
    for file_path in overall_stat_dir.glob("*/*/*/*/*"):
        with file_path.open("r") as file_handle:
            metadata_dict = load(file_handle)

        info_dict = get_info_from_metadata_file_path(file_path)
        info_dict["sample_blk_err"]= metadata_dict["sample_percent_err"]["mean"]
        info_dict["post_blk_err"]= metadata_dict["postprocess_percent_err"]["mean"]
        overall_row_list.append(info_dict)
    
    return DataFrame(hrc_row_list), DataFrame(overall_row_list)


def plot_all():
    hrc_df, overall_df = load_data()

    for group_index, group_df in hrc_df.groupby(["rate"]):
        print(group_index)
        print(group_df)

    print(hrc_df)
    print(overall_df)
    





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


def scatter_plot_hrc_error(config: GlobalConfig):
    metadata_dir = config.postprocess_hrc_err_dir_path

    label_arr = []
    sample_overall_hrc_err_arr, postprocess_overall_hrc_err_arr = [], []
    sample_read_hrc_err_arr, postprocess_read_hrc_err_arr = [], []
    sample_read_only_hrc_err_arr, postprocess_read_only_hrc_err_arr = [], []

    for file_path in metadata_dir.glob("*/*/*/*/*"):
        with file_path.open("r") as file_handle:
            metadata_dict = load(file_handle)

        file_name_split = file_path.stem.split("_")
        rate, bits, seed = int(file_name_split[0]), int(file_name_split[1]), int(file_name_split[2])
        
        sample_overall_hrc_err_arr.append(metadata_dict["samp_2"]["mean"])
        postprocess_overall_hrc_err_arr.append(metadata_dict["post_2"]["mean"])

        sample_read_hrc_err_arr.append(metadata_dict["samp_1"]["mean"])
        postprocess_read_hrc_err_arr.append(metadata_dict["post_1"]["mean"])

        sample_read_only_hrc_err_arr.append(metadata_dict["samp_3"]["mean"])
        postprocess_read_only_hrc_err_arr.append(metadata_dict["post_3"]["mean"])
        label_arr.append(rate)

    scatter_plot(sample_overall_hrc_err_arr, postprocess_overall_hrc_err_arr, label_arr, 
                    "Sample Only Mean Hit Rate Error (%)", 
                    "Sample + BlkSample Only Mean Hit Rate Error (%)", "./files/post_process_scatter/scatter_overall_hr.pdf")

    scatter_plot(sample_read_hrc_err_arr, postprocess_read_hrc_err_arr, label_arr, 
                    "Sample Only Mean Hit Rate Error (%)", 
                    "Sample + BlkSample Only Mean Hit Rate Error (%)", "./files/post_process_scatter/scatter_read_hr.pdf")

    scatter_plot(sample_read_only_hrc_err_arr, postprocess_read_only_hrc_err_arr, label_arr, 
                    "Sample Only Mean Hit Rate Error (%)", 
                    "Sample + BlkSample Only Mean Hit Rate Error (%)", "./files/post_process_scatter/scatter_read_only_hr.pdf")


def scatter_plot_post_processing_error(config: GlobalConfig):
    metadata_dir = config.postprocess_stat_dir_path

    post_processing_err_arr = []
    sample_err_arr, label_arr = [], []
    sampling_std_arr, postprocess_std_arr = [], [] 
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

    parser = ArgumentParser(description="Create scatter plots from the output of sampling and post-processing.") 
    args = parser.parse_args()

    # scatter_plot_post_processing_error(global_config)
    # scatter_plot_hrc_error(global_config)
    plot_all()


if __name__ == "__main__":
    main()