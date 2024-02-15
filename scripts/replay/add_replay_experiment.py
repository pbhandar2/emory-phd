from pathlib import Path 
from argparse import ArgumentParser
from pandas import DataFrame, read_csv 
from os import environ 

from cydonia.util.S3Client import S3Client
from get_sample_sizes import get_tier_size_list
from lib import get_num_blocks_full

REPLAY_EXPERIMENT_FILE = Path("files/experiments.csv")
SAMPLE_TYPE = "basic"
WORKLOAD_TYPE = "cp"
KEY_BASE = "mtcachedata/new-replay/traces"
CUR_OUTPUT_SET = "set-0"
REPLAY_EXPERIMENT_REMOTE_FILE_KEY = "mtcachedata/new-replay/experiments.csv"


def add_experiments(cache_trace_path):
    if "post_process" in str(cache_trace_path.absolute()):
        print("Adding a post process file.")
        split_path = str(cache_trace_path.absolute()).split("/")
        workload_name = split_path[-3]
        remote_file_name = "-".join(split_path[-4:])
        sample_upload_key = "{}/{}-{}/{}".format(KEY_BASE, WORKLOAD_TYPE, SAMPLE_TYPE, remote_file_name)
        full_upload_key = "{}/{}/{}.csv".format(KEY_BASE, WORKLOAD_TYPE, workload_name)
    elif "cp-" in str(cache_trace_path.absolute()):
        print("Adding a sample file.")
        split_path = str(cache_trace_path.absolute()).split("/")
        workload_name = split_path[-2]
        remote_file_name = "-".join(split_path[-2:])
        sample_upload_key = "{}/{}-{}/{}".format(KEY_BASE, WORKLOAD_TYPE, SAMPLE_TYPE, remote_file_name)
        full_upload_key = "{}/{}/{}.csv".format(KEY_BASE, WORKLOAD_TYPE, workload_name)
    else:
        raise ValueError("Check the file, is it a sample?")

    sample_num_blocks, tier_size_list = get_tier_size_list(cache_trace_path)
    sample_rate = sample_num_blocks/get_num_blocks_full(workload_name)

    print("Current sampling rate is {}".format(sample_rate))
    experiment_dict_list = []
    for t1_size, t2_size in tier_size_list:
        sample_experiment_dict = {}
        sample_experiment_dict["t1"] = t1_size
        sample_experiment_dict["t2"] = t2_size 
        sample_experiment_dict["traceKey"] = sample_upload_key

        full_experiment_dict = {}
        full_experiment_dict["t1"] = int(t1_size/sample_rate)
        full_experiment_dict["t2"] = int(t2_size/sample_rate)
        full_experiment_dict["traceKey"] = full_upload_key

        experiment_dict_list.append(sample_experiment_dict)
        experiment_dict_list.append(full_experiment_dict)
    
    experiment_df = DataFrame(experiment_dict_list)
    experiment_df.to_csv(REPLAY_EXPERIMENT_FILE, mode="a", header=not REPLAY_EXPERIMENT_FILE.exists(), index=False)

    new_experiment_df = read_csv(REPLAY_EXPERIMENT_FILE)
    new_experiment_df = new_experiment_df.drop_duplicates()
    new_experiment_df.to_csv(REPLAY_EXPERIMENT_FILE, header=True, index=False)

    print("Uploading now", REPLAY_EXPERIMENT_REMOTE_FILE_KEY, str(REPLAY_EXPERIMENT_FILE.absolute()))
    s3_client = S3Client(environ["AWS_KEY"], environ["AWS_SECRET"], environ["AWS_BUCKET"])
    s3_client.upload_s3_obj(REPLAY_EXPERIMENT_REMOTE_FILE_KEY, str(REPLAY_EXPERIMENT_FILE.absolute()))




def main():
    parser = ArgumentParser(description="Get tier sizes to be replayed given a cache trace.")
    parser.add_argument("cache_trace_path", type=Path, help="Path of cache trace.")
    args = parser.parse_args()

    if not args.cache_trace_path.exists():
        print("{} does not exist.".format(args.cache_trace_path))
        return 
    
    add_experiments(args.cache_trace_path)


if __name__ == "__main__":
    main()