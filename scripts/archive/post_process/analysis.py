from pandas import read_csv
from argparse import ArgumentParser

from keyuri.config.BaseConfig import BaseConfig


def main():
    parser = ArgumentParser(description="Analyze output file of post-processing algorithm.")
    parser.add_argument("--sample_type", type=str, default="iat", help="The type of sampling technique used.")
    parser.add_argument("--algo_name", type=str, default="best", help="The post-processing algorithm used.")
    args = parser.parse_args()

    config = BaseConfig()
    algo_output_dir_path = config.get_algo_output_dir_path(args.algo_name, args.sample_type)

    for workload_output_dir_path in algo_output_dir_path.iterdir():
        for output_file_path in workload_output_dir_path.iterdir():
            algo_output_df = read_csv(output_file_path)
            print(output_file_path)
            print(algo_output_df)
    

if __name__ == "__main__":
    main()