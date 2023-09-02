from pathlib import Path 
from argparse import ArgumentParser

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig
from keyuri.analysis.SampleMetadata import SampleMetadata


def main():
    global_config = GlobalConfig()
    
    parser = ArgumentParser("Plot feature while varying bits.")
    parser.add_argument("workload_name", help="Name of the workload")
    parser.add_argument("--workload_type", default="cp", help="Type of the workload")
    parser.add_argument("--source_dir_path", type=Path, default=global_config.source_dir_path, help="Source directory of all data.")
    args = parser.parse_args()

    if args.source_dir_path != global_config.source_dir_path:
        global_config.update_source_dir(args.source_dir_path) 
    
    print("Source dir: {}".format(global_config.source_dir_path))
    sample_metadata = SampleMetadata(args.workload_name, global_config=global_config)

    sample_metadata = SampleMetadata(args.workload_name)
    sample_metadata.print_best_samples()


if __name__ == "__main__":
    main()