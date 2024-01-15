from argparse import ArgumentParser
from pathlib import Path 
from pandas import read_csv, DataFrame
from json import loads, load
from numpy import mean 
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter


from keyuri.config.BaseConfig import BaseConfig
from cydonia.profiler.CacheTrace import CacheTraceReader


err_header_arr = ["cur_mean_read_size", "cur_mean_write_size", "cur_mean_read_iat",
                    "cur_mean_write_iat", "misalignment_per_read", "misalignment_per_write", "write_ratio"]


def block_subplot(ax: plt.Axes, data_df: DataFrame, op_type: str):
    size_arr = data_df["cur_mean_{}_size".format(op_type)].to_list()
    iat_arr = data_df["cur_mean_{}_iat".format(op_type)].to_list()
    misalignment_arr = data_df["misalignment_per_{}".format(op_type)].to_list()
    ratio_arr = data_df["{}_ratio".format(op_type)].to_list()


    # mean_arr = data_df["mean"].to_list()
    # hit_rate_error_arr = data_df["mean_{}_hr".format(op_type)].to_list()
    # write_ratio_arr = data_df["write_ratio"].to_list()
    num_iter_arr = data_df["num_iter"].to_list()

    markersize_val = 15
    alpha_val = 0.4
    ax.plot(num_iter_arr, size_arr, 'b-*', markersize=markersize_val, alpha=alpha_val, label="Size")
    ax.plot(num_iter_arr, iat_arr, 'g-^', markersize=markersize_val, alpha=alpha_val, label="IAT")
    ax.plot(num_iter_arr, misalignment_arr, 'r-8', markersize=markersize_val, alpha=alpha_val, label="Misalignment")
    ax.plot(num_iter_arr, ratio_arr, 'c-X', markersize=markersize_val, alpha=alpha_val, label="Ratio")
    # ax.plot(num_iter_arr, hit_rate_error_arr, 'c-p', markersize=markersize_val, alpha=alpha_val, label="MRC")
    # ax.plot(num_iter_arr, mean_arr, 'y-s', markersize=markersize_val, alpha=alpha_val, label="Mean")

    # if op_type == "write":
    #     ax.plot(num_iter_arr, write_ratio_arr, 'm-P', markersize=markersize_val, alpha=0.5, label="Write Ratio")


def plot_hr(ax: plt.Axes, data_df: DataFrame):
    num_iter_arr = data_df["num_iter"].to_list()
    read_hr_arr = hit_rate_error_arr = data_df["mean_read_hr"].to_list()
    write_hr_arr = hit_rate_error_arr = data_df["mean_write_hr"].to_list()
    overall_hr_arr = hit_rate_error_arr = data_df["mean_overall_hr"].to_list()

    markersize_val = 15
    alpha_val = 0.4
    ax.plot(num_iter_arr, read_hr_arr, 'b-H', markersize=markersize_val, alpha=alpha_val, label="Read")
    ax.plot(num_iter_arr, write_hr_arr, 'g-D', markersize=markersize_val, alpha=alpha_val, label="Write")
    ax.plot(num_iter_arr, overall_hr_arr, 'r-P', markersize=markersize_val, alpha=alpha_val, label="Overall")
    ax.plot([], [], 'y-s', markersize=markersize_val, alpha=alpha_val, label="Mean Error")



def plot_mean(ax: plt.Axes, data_df: DataFrame):
    num_iter_arr = data_df["num_iter"].to_list()
    mean_arr = data_df["mean"].to_list()

    markersize_val = 15
    alpha_val = 0.4
    ax.plot(num_iter_arr, mean_arr, 'y-s', markersize=markersize_val, alpha=alpha_val, label="Mean Error")


def plot(data_df: DataFrame, plot_path: Path, title_str: str ):
    plt.rcParams.update({'font.size': 22})
    fig, axs = plt.subplots(nrows=2, ncols=2, figsize=[14,10])

    block_subplot(axs[0][0], data_df, "read")
    block_subplot(axs[0][1], data_df, "write")

    title_font_size = 18
    axs[0][0].set_title("Read", fontsize=title_font_size)
    axs[0][1].set_title("Write", fontsize=title_font_size)
    axs[1][0].set_title("Mean Hit Rate Error", fontsize=title_font_size)
    axs[1][1].set_title("Mean Error", fontsize=title_font_size)

    axs[0][0].legend(loc="upper center", 
                        bbox_to_anchor=(1.2, -0.15),
                        ncol=4)

    plot_hr(axs[1][0], data_df)
    plot_mean(axs[1][1], data_df)

    axs[1][0].legend(loc="upper center", 
                        bbox_to_anchor=(1.2, 1.4),
                        ncol=4)
    plt.subplots_adjust(hspace=0.8)

    fig.text(0.04, 0.5, 'Percent Error (%)', va='center', rotation='vertical')
    fig.text(0.4, 0.04, 'Number of Iterations', va='center', rotation='horizontal')
    fig.text(0.4, 1.7, title_str, va="center", rotation="horizontal")
    fig.suptitle(title_str)
    
    plot_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(plot_path)
    plt.close(fig)


def load_data(pp_output_file_path: Path, 
              pp_hit_rate_error_file_list: list,
              sample_error_file_path: Path,
              hit_rate_error_file_path: Path):
    output_df = read_csv(pp_output_file_path)
    hit_rate_error_df = read_csv(hit_rate_error_file_path)

    init_error_dict = {}
    init_error_dict["mean_read_hr"] = hit_rate_error_df["percent_read_hr"].mean()
    init_error_dict["mean_write_hr"] = hit_rate_error_df["percent_write_hr"].mean()
    init_error_dict["mean_overall_hr"] = hit_rate_error_df["percent_overall_hr"].mean()

    with sample_error_file_path.open("r") as handle:
        sample_error_dict = load(handle)
    
    for error_key_name in err_header_arr:
        init_error_dict[error_key_name] = sample_error_dict[error_key_name]
    
    err_arr = [abs(init_error_dict[key]) for key in init_error_dict]
    init_error_dict["mean"] = mean(err_arr)
    init_error_dict["num_iter"] = 0
    init_error_dict["read_ratio"] = 1 - init_error_dict["write_ratio"]

    data_dict_arr = [init_error_dict]
    for pp_hit_rate_error_file_path in pp_hit_rate_error_file_list:
        data_dict = {}
        num_iter = int(pp_hit_rate_error_file_path.parent.name)
        data_dict.update(loads(output_df.iloc[num_iter - 1][err_header_arr].to_json()))
        data_dict["read_ratio"] = 1 - data_dict["write_ratio"]

        hit_rate_error_df = read_csv(pp_hit_rate_error_file_path)
        data_dict["mean_read_hr"] = hit_rate_error_df["percent_read_hr"].mean()
        data_dict["mean_write_hr"] = hit_rate_error_df["percent_write_hr"].mean()
        data_dict["mean_overall_hr"] = hit_rate_error_df["percent_overall_hr"].mean()

        err_arr = [abs(data_dict[key]) for key in data_dict]
        data_dict["mean"] = mean(err_arr)
        data_dict["num_iter"] = num_iter
        assert data_dict["num_iter"] > 0, "Number of iteration has to be greater than 0."

        data_dict_arr.append(data_dict)
    return DataFrame(data_dict_arr).sort_values(by=["num_iter"])


def main():
    parser = ArgumentParser(description="Plot mean error of the algorithm.")

    parser.add_argument("--workload",
                            "-w", 
                            type=str, 
                            help="Name of workload.")

    parser.add_argument("--type",
                            "-t", 
                            type=str, 
                            default="basic",
                            help="The sample type.")
    
    parser.add_argument("--metric",
                            "-m", 
                            type=str, 
                            help="The metric used by the post-processing algorithm.")

    parser.add_argument("--rate",
                            "-r",
                            type=float, 
                            help="Sampling rate between (0 and 1).")

    parser.add_argument("--seed",
                            "-s",
                            type=int, 
                            help="Random seed of the sample.")

    parser.add_argument("--bits",
                            "-b",
                            type=int, 
                            help="Number of lower order bits of addresses ignored.")
    
    parser.add_argument("--abits",
                            "-a",
                            type=int, 
                            help="Number of lower order bits of addresses ignored.")

    parser.add_argument("-o",
                            "--output_dir",
                            type=str,
                            default="./files/multi_err_plot",
                            help="The output directory where plots are stored.")
    
    args = parser.parse_args()

    base_config = BaseConfig()
    pp_hit_rate_error_file_list = base_config.get_all_post_process_hit_rate_error_files(args.type,
                                                                                            args.workload,
                                                                                            args.metric,
                                                                                            args.abits,
                                                                                            int(100*args.rate),
                                                                                            args.bits,
                                                                                            args.seed)
    
    print(pp_hit_rate_error_file_list)
    if len(pp_hit_rate_error_file_list) < 2:
        print("Too little points to plot.")
        return 
    
    pp_output_file_path = base_config.get_sample_post_process_output_file_path(args.type,
                                                                                args.workload,
                                                                                args.metric,
                                                                                args.abits,
                                                                                int(100*args.rate),
                                                                                args.bits,
                                                                                args.seed)
    
    if not pp_output_file_path.exists():
        print("Output file {} does not exist. Exiting.".format(pp_output_file_path))
        return 


    sample_error_file_path = base_config.get_sample_block_error_file_path(args.type,
                                                                            args.workload,
                                                                            int(100 * args.rate),
                                                                            args.bits,
                                                                            args.seed)
    
    if not sample_error_file_path.exists():
        print("Sample error file {} does not exist. Exiting.".format(sample_error_file_path))
        return 
    
    sample_hit_rate_error_file_path = base_config.get_hit_rate_error_file_path(args.type,
                                                                               args.workload,
                                                                               int(100 * args.rate),
                                                                               args.bits,
                                                                               args.seed)
    
    if not sample_hit_rate_error_file_path.exists():
        print("Sample hit rate error file {} exists. Exiting.".format(sample_hit_rate_error_file_path))
        return 
    
    plot_path = Path(args.output_dir).joinpath("{}/{}/{}/{}_{}_{}_{}.png".format(base_config.get_compound_workload_set_name(args.type), 
                                                                        args.workload, 
                                                                        args.metric,
                                                                        int(100 * args.rate),
                                                                        args.bits,
                                                                        args.seed,
                                                                        args.abits))

    data_df = load_data(pp_output_file_path, 
                        pp_hit_rate_error_file_list,
                        sample_error_file_path,
                        sample_hit_rate_error_file_path)

    sample_cache_trace_path = base_config.get_sample_cache_trace_path(args.type, args.workload, int(100*args.rate), args.bits, args.seed)
    cache_trace_reader = CacheTraceReader(sample_cache_trace_path)
    sample_num_blocks = len(cache_trace_reader.get_unscaled_unique_block_addr_set())
    full_num_blocks = int(sample_num_blocks/args.rate)

    data_df["i"] = range(len(data_df))
    data_df["rate"] = (sample_num_blocks - data_df["i"])/full_num_blocks

    print(data_df["rate"].to_list()[::10])
    print(data_df)

    title_str = "{}, start rate: {}, end rate: {:4f}".format(args.workload, args.rate, data_df.iloc[-1]["rate"])
 
    plot(data_df, plot_path, title_str)


if __name__ == "__main__":
    main()