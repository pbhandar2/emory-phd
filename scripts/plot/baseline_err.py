from argparse import ArgumentParser
from pathlib import Path 
from pandas import read_csv, DataFrame
from json import load

import matplotlib.ticker as ticker
import matplotlib.pyplot as plt

from keyuri.config.BaseConfig import BaseConfig


def plot(data_df: DataFrame, plot_path: Path) -> None:
    plt.rcParams.update({'font.size': 37})
    fig, ax = plt.subplots(figsize=[28,10])

    print(data_df.columns.values)

    ax.boxplot(data_df[["cur_mean_read_size", 
                        "cur_mean_write_size",
                        "cur_mean_read_iat",
                        "cur_mean_write_iat",
                        "misalignment_per_read",
                        "misalignment_per_write",
                        "write_ratio",
                        "read_hr",
                        "write_hr",
                        "overall_hr"]], showfliers=False)
    
    ax.axhline(y=0, linestyle='--')
    
    ax.set_xticks(range(1, 11))
    ax.set_xticklabels(["Read \nSize", 
                        "Write \nSize",
                        "Read \nIAT",
                        "Write \nIAT",
                        "Read \nMisalign",
                        "Write \nMisalign",
                        "Write \nRatio",
                        "Read \nHit Rate",
                        "Write \nHit Rate",
                        "Overall \nHit Rate"])
    ax.set_ylabel("Percent Error (%)")

    plt.tight_layout()
    plot_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(plot_path)
    plt.close(fig)


def load_data(
        config: BaseConfig, 
        sample_type: str
) -> DataFrame:
    err_dict_arr = []
    sample_error_file_list = config.get_all_sample_error_file_path(sample_type)
    for sample_error_file_path in sample_error_file_list:
        workload_name = sample_error_file_path.parent.name 
        rate, bits, seed = config.get_sample_file_info(sample_error_file_path)
        hit_rate_error_file_path = config.get_hit_rate_error_file_path(sample_type, workload_name, rate, bits, seed)

        if not hit_rate_error_file_path.exists():
            continue 
        
        err_dict = {}
        hit_rate_error_df = read_csv(hit_rate_error_file_path)
        err_dict["read_hr"] = hit_rate_error_df["percent_read_hr"].mean()
        err_dict["write_hr"] = hit_rate_error_df["percent_write_hr"].mean()
        err_dict["overall_hr"] = hit_rate_error_df["percent_overall_hr"].mean()
        err_dict["workload"] = workload_name 

        with sample_error_file_path.open("r") as handle:
            sample_err_dict = load(handle)
        
        err_dict.update(sample_err_dict)
        err_dict_arr.append(err_dict)
        print(err_dict)
    
    return DataFrame(err_dict_arr)


def main():
    parser = ArgumentParser("Plot the error in sample features for different lower order bits ignored.")
    
    parser.add_argument("--bits",
                            "-b",
                            type=int, 
                            required=True,
                            help="Number of lower address bits ignored.")
    
    parser.add_argument("--rate",
                            "-r",
                            type=float, 
                            help="Sampling rate between (0 and 1).")

    parser.add_argument("--type",
                            "-t", 
                            type=str, 
                            default="basic",
                            help="The sample type.")

    parser.add_argument("--seed",
                            "-s",
                            type=int, 
                            default=42,
                            help="Random seed of the sample.")
    
    parser.add_argument("--output_dir",
                            "-o",
                            default=Path("./files/baseline_err"),
                            type=Path,
                            help="Directory where plots will be generated.")
    
    parser.add_argument("--source_dir",
                            "-sd",
                            type=Path,
                            help="The directory with the source data.")
    
    parser.add_argument("--datafile",
                            "-df",
                            type=Path,
                            default=Path("./data/baseline.csv"),
                            help="The DataFrame containing all the data needed to plot.")
    
    parser.add_argument("--loadfile",
                            "-lf",
                            action='store_true',
                            help="Flag indicating if data to be loaded from file, otherwise computed.")

    args = parser.parse_args()

    config = BaseConfig() if args.source_dir is None else BaseConfig(source_dir_path=args.source_dir)

    if args.loadfile:
        data_df = read_csv(args.datafile)
    else:
        data_df = load_data(config, args.type)
        data_df.to_csv(args.datafile, index=False)
    
    print(data_df)
    plot_path = args.output_dir.joinpath("{}/{}_{}_{}.pdf".format(args.type, args.bits, args.seed, int(100*args.rate)))
    plot(data_df[(data_df["bits"] == 0) & (data_df["rate"] == int(100*args.rate))], plot_path)
    

if __name__ == "__main__":
    main()