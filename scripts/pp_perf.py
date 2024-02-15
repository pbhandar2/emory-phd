from argparse import ArgumentParser
from pandas import read_csv 

from keyuri.config.BaseConfig import BaseConfig


def main():
    parser = ArgumentParser(description="Measure the performance of PP.")
    parser.add_argument("-s", "--start_r",
                            type=float,
                            help="The starting sampling rate.")
    parser.add_argument("-e", "--end_r",
                            type=float,
                            help="The end sampling rate.")
    args = parser.parse_args()

    config = BaseConfig()
    sample_set_name = "basic"
    metric_name = "mean"
    algo_bits = 4
    bits = 4 
    seed = 42 

    output_file_list = config.get_all_sample_post_process_output_file_path(sample_set_name, 
                                                                            metric_name, 
                                                                            algo_bits, 
                                                                            int(100*args.start_r),
                                                                            bits,
                                                                            seed)
    

    for output_file_path in output_file_list:
        output_df = read_csv(output_file_path)
        filter_df = output_df[output_df["rate"] <= args.end_r]
        if not len(filter_df):
            continue 

        output_df = output_df[output_df["rate"] >= args.end_r]

        print(output_df)
        print(output_file_path)
        print(len(output_df))

        workload_name = output_file_path.parent.name 
        rate, seed, bits = config.get_sample_file_info(output_file_path)

        pp_hit_rate_err_file_path = config.get_post_process_hit_rate_error_file_path(sample_set_name,
                                                                                     workload_name,
                                                                                     metric_name,
                                                                                     algo_bits,
                                                                                     rate,
                                                                                     bits,
                                                                                     seed,
                                                                                     len(output_df)-1)
        print(pp_hit_rate_err_file_path, pp_hit_rate_err_file_path.exists())




if __name__ == "__main__":
    main()