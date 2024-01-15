"""Compute hit rate error by comparing reuse distance histograms of full and sample trace."""

from argparse import ArgumentParser

from keyuri.experiments.HitRateError import MultiHitRateError
from keyuri.config.BaseConfig import BaseConfig


def main():
    parser = ArgumentParser(description="Compute hit rate error by comparing reuse distance histograms generated from full and sample traces.")
    parser.add_argument("workload_name", type=str, help="Name of the workload.")
    parser.add_argument("--workload_type", type=str, default="cp", help="Type of workload.")
    parser.add_argument("--sample_type", "-t", type=str, default="iat", help="Type of sample.")
    args = parser.parse_args()

    global_config = BaseConfig()
    full_rd_hist_file_path = global_config.get_rd_hist_file_path(args.workload_name)
    if not full_rd_hist_file_path.exists():
        print("Full rd hist {} does not exist. Creating one..".format(full_rd_hist_file_path))
        return 

    sample_rd_hist_dir_path = global_config.get_sample_rd_hist_dir_path(args.sample_type, args.workload_name)
    hit_rate_err_dir_path = global_config.get_hit_rate_error_data_dir_path(args.sample_type, args.workload_name)

    hit_rate_err_dir_path.mkdir(exist_ok=True, parents=True)
    multi_rd_hist_analysis = MultiHitRateError(full_rd_hist_file_path, sample_rd_hist_dir_path, hit_rate_err_dir_path)
    multi_rd_hist_analysis.generate_hit_rate_err_files()


if __name__ == "__main__":
    main()