from argparse import ArgumentParser
from pathlib import Path 
from pandas import read_csv 

from keyuri.analysis.SampleMetadata import SampleMetadata

from keyuri.config.Config import GlobalConfig


def main():
    global_config = GlobalConfig()

    parser = ArgumentParser(description="Generate a dataframe containing features of the full trace and its samples.")
    parser.add_argument("workload_name", type=str, help="Name of the workload.")
    parser.add_argument("--workload_type", default="cp", type=str, help="Workload type.")
    parser.add_argument("--source_dir_path", type=Path, default=global_config.source_dir_path, help="Source directory of all data.")
    parser.add_argument("--batch_size", type=int, default=4, help="Number of processes to spawn for processing.")
    parser.add_argument("--force", default=False, type=bool, help="Boolean indicating whether to force recomputation even if file exists.")
    args = parser.parse_args()

    if args.source_dir_path != global_config.source_dir_path:
        global_config.update_source_dir(args.source_dir_path) 
    
    print("Source dir: {}".format(global_config.source_dir_path))
    sample_metadata = SampleMetadata(args.workload_name, global_config=global_config)

    cumulative_feature_file_path = global_config.get_cumulative_feature_file_path(args.workload_type, args.workload_name)
    if not cumulative_feature_file_path.exists():
        cumulative_feature_file_path.parent.mkdir(exist_ok=True, parents=True)
        sample_metadata.generate_cumulative_feature_file(cumulative_feature_file_path)
    else:
        feature_df = read_csv(cumulative_feature_file_path)
        if "read_hr_1.0" not in list(feature_df.columns) or args.force:
            print("Cumulative feature file is outdated! Recreating {}".format(cumulative_feature_file_path))
            sample_metadata.generate_cumulative_feature_file(cumulative_feature_file_path)
        else:
            print("Cumulative feature file already exists! {}".format(cumulative_feature_file_path))


if __name__ == "__main__":
    main()

