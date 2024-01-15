from pathlib import Path 
import matplotlib.pyplot as plt
from collections import defaultdict
from pandas import read_csv 
from argparse import ArgumentParser 

from keyuri.config.Config import GlobalConfig


def plot_hr_err_vs_rate(
        workload_name: str,
        workload_type: str, 
        sample_type: str 
) -> None:
    global_config = GlobalConfig()
    overall_data_dict, read_data_dict, write_data_dict = {}, {}, {}
    hr_err_dir_path = global_config.sample_hit_rate_err_dir_path.joinpath(sample_type, workload_type, workload_name)

    for hr_err_file_path in hr_err_dir_path.iterdir():
        split_file_name = hr_err_file_path.stem.split("_")
        rate, bits, seed = int(split_file_name[0]), int(split_file_name[1]), int(split_file_name[2])
        df = read_csv(hr_err_file_path)

        if bits > 0:
            continue 

        mean_overall_hr = df["percent_overall_hr"].mean()
        mean_read_hr = df["percent_read_hr"].mean()
        mean_write_hr = df["percent_write_hr"].mean()
        if seed not in overall_data_dict:
            overall_data_dict[seed] = [[rate, mean_overall_hr]]
            read_data_dict[seed] = [[rate, mean_read_hr]]
            write_data_dict[seed] = [[rate, mean_write_hr]]
        else:
            overall_data_dict[seed].append([rate, mean_overall_hr])
            read_data_dict[seed].append([rate, mean_read_hr])
            write_data_dict[seed].append([rate, mean_write_hr])
    
    print(overall_data_dict)
    plt.rcParams.update({'font.size': 22})
    fig, ax = plt.subplots(figsize=[14,10])

    for seed in overall_data_dict:
        sorted_array = sorted(overall_data_dict[seed], key=lambda k: k[0])
        x = [_[0] for _ in sorted_array]
        y = [_[1] for _ in sorted_array]
        plt.plot(x, y, '-*', label=seed)

    output_path = Path("./files/hr_err_vs_rate/hr_err_vs_rate_{}.png".format(workload_name))
    output_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_path)
    plt.close(fig)
        



def main():
    parser = ArgumentParser(description="Plot accuracy of random spatial sampling for different set of samples.")
    parser.add_argument("workload_name", type=str, help="Name of the workload.")
    parser.add_argument("--workload_type", type=str, default="cp", help="Type of workload.")
    parser.add_argument("--sample_type", type=str, default="iat", help="Type of sample.")
    parser.add_argument("--algo_type", type=str, help="The post-processing algorithm.")
    args = parser.parse_args()
    plot_hr_err_vs_rate(args.workload_name, args.workload_type, args.sample_type)


if __name__ == "__main__":
    main()