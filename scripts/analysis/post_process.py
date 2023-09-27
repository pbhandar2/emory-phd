from pandas import read_csv, option_context
from pathlib import Path 
from argparse import ArgumentParser
from json import load, dumps, dump

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


def main():
    global_config = GlobalConfig()
    sample_config = SampleExperimentConfig()
    
    parser = ArgumentParser(description="Post process sample block traces.")
    parser.add_argument("workload_name", type=str, help="The name of the workload.")
    parser.add_argument("rate", type=int, help="The rate of sampling.")
    parser.add_argument("seed", type=int, help="Random seed.")
    parser.add_argument("bits", type=int, help="Number of lower order bits of addresses that are ignored.")
    parser.add_argument("max_rate", type=int, help="Maximum effective sample rate after post processing.")
    parser.add_argument("--sample_type", type=str, default="iat", help="The type of sampling technique used.")
    parser.add_argument("--workload_type", type=str, default="cp", help="The type of workload.")
    parser.add_argument("--source_dir_path", type=Path, default=global_config.source_dir_path, help="Source directory of all data.")
    parser.add_argument("--priority", type=str, default="delta_err", help="The priority metric.")
    args = parser.parse_args()


if __name__ == "__main__":
    main()