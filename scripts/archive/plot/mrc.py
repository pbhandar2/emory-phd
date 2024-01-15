from pathlib import Path 
from argparse import ArgumentParser
import matplotlib.pyplot as plt
from numpy import mean 

from keyuri.analysis.PlotMRC import get_hrc
from keyuri.config.BaseConfig import BaseConfig


def hrc_mae(hrc_arr, sample_hrc_arr, sample_rate):
    err_arr = []
    max_cache_size = 0
    for cur_size in range(len(sample_hrc_arr)):
        sample_size = int(cur_size/sample_rate)

        hrc_size = sample_size 
        if sample_size >= len(hrc_arr):
            break

        hr_diff = abs(hrc_arr[hrc_size] - sample_hrc_arr[cur_size])
        if hr_diff > 0:
            percent_err = 100*(hr_diff)/hrc_arr[hrc_size]
        else:
            percent_err = 0
        max_cache_size = hrc_size
        err_arr.append(percent_err)
    return float(mean(err_arr)), max_cache_size


def plot_sample(read_hrc_arr, write_hrc_arr, overall_hrc_arr, sample_read_hrc_arr, sample_write_hrc_arr, sample_overall_hrc_arr, sample_rate, output_path):
    output_path.parent.mkdir(exist_ok=True, parents=True)

    fig, axs = plt.subplots(3, 1, figsize=[14,10])
    axs[0].plot(range(len(read_hrc_arr)), read_hrc_arr, label="Full")
    axs[0].plot([int(cur_size/sample_rate) for cur_size in range(len(sample_read_hrc_arr))], sample_read_hrc_arr, label="Sample")
    axs[1].plot(range(len(write_hrc_arr)), write_hrc_arr, label="Full")
    axs[1].plot([int(cur_size/sample_rate) for cur_size in range(len(sample_write_hrc_arr))], sample_write_hrc_arr, label="Sample")
    axs[2].plot(range(len(overall_hrc_arr)), overall_hrc_arr, label="Full")
    axs[2].plot([int(cur_size/sample_rate) for cur_size in range(len(sample_overall_hrc_arr))], sample_overall_hrc_arr, label="Sample")

    read_hrc_mae, max_read_cache_size = hrc_mae(read_hrc_arr, sample_read_hrc_arr, sample_rate)
    write_hrc_mae, max_write_cache_size = hrc_mae(write_hrc_arr, sample_write_hrc_arr, sample_rate)
    overall_hrc_mae, max_overall_cache_size = hrc_mae(overall_hrc_arr, sample_overall_hrc_arr, sample_rate)
    title_str = "Read: {:.3f} Write: {:.3f} Overall: {:.3f}".format(read_hrc_mae, write_hrc_mae, overall_hrc_mae)

    x_lim_val = max(max_read_cache_size, max_write_cache_size, max_overall_cache_size)
    axs[0].set_xlim(left=-1*int(x_lim_val*0.01), right=x_lim_val)
    axs[1].set_xlim(left=-1*int(x_lim_val*0.01), right=x_lim_val)
    axs[2].set_xlim(left=-1*int(x_lim_val*0.01), right=x_lim_val)

    axs[0].set_title(title_str, fontsize=16)
    axs[2].set_xlabel('Cache Size (4KB blocks)')
    axs[1].set_ylabel('Hit Rate')

    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)


def plot(read_hrc_arr, write_hrc_arr, overall_hrc_arr, output_path):
    output_path.parent.mkdir(exist_ok=True, parents=True)

    fig, axs = plt.subplots(3, 1, figsize=[14,10])
    axs[0].plot(range(len(read_hrc_arr)), read_hrc_arr)
    axs[1].plot(range(len(write_hrc_arr)), write_hrc_arr)
    axs[2].plot(range(len(overall_hrc_arr)), overall_hrc_arr)
    fig.text(0.5, 0.04, 'Cache Size (4KB blocks)', ha='center')
    axs[1].set_ylabel('Hit Rate')
    plt.savefig(output_path)
    plt.close(fig)


def main():
    parser = ArgumentParser(description="Plot MRC from reuse distance histograms.")
    parser.add_argument("workload", type=str, help="Name of the workload.")
    parser.add_argument("--output_dir", "-o", type=Path, default=Path("./files/mrc"), help="Output directory of plots.")
    parser.add_argument("--sample_type", "-s", type=str, default="none", help="The type of sample.")
    args = parser.parse_args()

    config = BaseConfig()
    rd_hist_file_path = config.get_rd_hist_file_path(args.workload)
    read_hrc_arr, write_hrc_arr, overall_hrc_arr = get_hrc(rd_hist_file_path)

    plt.rcParams.update({'font.size': 22})

    if args.sample_type == "none":
        output_path = args.output_dir.joinpath(args.sample_type, "{}.png".format(args.workload))
        plot(read_hrc_arr, write_hrc_arr, overall_hrc_arr, output_path)
    else:
        sample_rd_hist_dir = config.get_sample_rd_hist_dir_path(args.sample_type, args.workload)
        for sample_rd_hist_file_path in sample_rd_hist_dir.iterdir():
            split_sample_file_name = sample_rd_hist_file_path.stem.split('_')
            rate, bits, seed = int(split_sample_file_name[0]), int(split_sample_file_name[1]), int(split_sample_file_name[2])
            output_path = args.output_dir.joinpath(args.sample_type, args.workload, "{}.png".format(sample_rd_hist_file_path.stem))
            sample_read_hrc_arr, sample_write_hrc_arr, sample_overall_hrc_arr = get_hrc(sample_rd_hist_file_path)
            plot_sample(read_hrc_arr, write_hrc_arr, overall_hrc_arr, sample_read_hrc_arr, sample_write_hrc_arr, sample_overall_hrc_arr, float(rate)/100, output_path)
            

if __name__ == "__main__":
    main()