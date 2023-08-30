from argparse import ArgumentParser
from pathlib import Path 

from keyuri.experiments.AnalyzeSampleFeatures import AnalyzeSampleFeatures

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


if __name__ == "__main__":
    global_config = GlobalConfig()
    sample_config = SampleExperimentConfig()
    
    parser = ArgumentParser(description="Create sample block traces.")
    parser.add_argument("workload_name", type=str, help="Name of the workload.")
    parser.add_argument("--workload_type", default="cp", type=str, help="Workload type.")
    parser.add_argument("--source_dir_path", type=Path, default=global_config.source_dir_path, help="Source directory of all data.")
    args = parser.parse_args()

    if args.source_dir_path != global_config.source_dir_path:
        global_config.source_dir_path = args.source_dir_path 

    analyze_samples = AnalyzeSampleFeatures(global_config=global_config, sample_config=sample_config)
    analyze_samples.analyze(args.workload_name, workload_type=args.workload_type)