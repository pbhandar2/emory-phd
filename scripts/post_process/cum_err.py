from argparse import ArgumentParser 
from pandas import read_csv, DataFrame

from keyuri.config.BaseConfig import BaseConfig


def main():
    parser = ArgumentParser(description="Compute the hit rate error of post processing algorithm.")

    parser.add_argument("--workload_type",
                            "-wt", 
                            default="cp",
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
                            default="mean",
                            help="The metric used by the post-processing algorithm.")

    parser.add_argument("--rate",
                            "-r",
                            type=float, 
                            help="Sampling rate between (0.0 and 1.0).")

    parser.add_argument("--seed",
                            "-s",
                            default=42,
                            type=int, 
                            help="Random seed of the sample.")

    parser.add_argument("--bits",
                            "-b",
                            default=4, 
                            type=int, 
                            help="Number of lower order bits of addresses ignored.")
    
    parser.add_argument("--abits",
                            "-a",
                            default=4, 
                            type=int, 
                            help="Number of lower order bits of addresses ignored.")

    parser.add_argument("--targetrate",
                            "-tr",
                            type=float, 
                            help="Target sampling rate.")
    
    parser.add_argument("-mi", "--min",
                            action='store_true',
                            help="The flag indicating to select min error.")
    
    args = parser.parse_args()

    config = BaseConfig()
    output_file_list = config.get_all_sample_post_process_output_file_path(args.type, args.metric, args.abits, int(100*args.rate), args.bits, args.seed)
    print(output_file_list)

    entry_list = []
    for output_file_path in output_file_list:
        output_df = read_csv(output_file_path)
        entry = {}
        entry["path"] = str(output_file_path.stem)
        entry["workload"] = str(output_file_path.parent.name)
        entry["start_rate"] = output_df.iloc[0]["rate"]
        entry["end_rate"] = output_df.iloc[-1]["rate"]
        
        target_len = len(output_df[output_df["rate"] >= args.targetrate]) 
        hr_err_file_path = config.get_pp_hit_rate_error_file_path(args.type, entry["workload"], args.metric, args.abits, 
                                                                    int(100*args.rate),
                                                                    args.bits,
                                                                    args.seed,
                                                                    target_len)
        if hr_err_file_path.exists():
            print(hr_err_file_path, " exists!")
            hr_df = read_csv(hr_err_file_path)
            mean_df = hr_df[["percent_read_hr", "percent_write_hr", "percent_overall_hr"]].mean()
            entry["r_hr"] = mean_df["percent_read_hr"]
            entry["w_hr"] = mean_df["percent_write_hr"]
            entry["o_hr"] = mean_df["percent_overall_hr"]
        
        entry["hr_file"] = hr_err_file_path.exists()
        last_row = output_df[output_df["rate"] >= args.targetrate].iloc[-1]
        entry["mean"] = last_row["mean"]
        
        print(entry["workload"])
        print(target_len)

        pp_hr_err_dir_path = config.get_pp_hit_rate_error_dir_path(args.type, entry["workload"], args.metric, args.abits, int(args.rate * 100), args.bits, args.seed)
        if pp_hr_err_dir_path.exists():
            print(len(list(pp_hr_err_dir_path.iterdir())), [_.stem for _ in list(pp_hr_err_dir_path.iterdir())])

        entry_list.append(entry)
    
    entry_df = DataFrame(entry_list)
    print(entry_df)


if __name__ == "__main__":
    main()
    