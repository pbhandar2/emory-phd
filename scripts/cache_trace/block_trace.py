from pathlib import Path 
from pandas import read_csv, DataFrame
from argparse import ArgumentParser 
from json import dumps, JSONEncoder
from numpy import ndarray, int64

from keyuri.config.BaseConfig import BaseConfig
# from cydonia.blksample.lib import get_workload_feature_dict_from_block_trace
# from cydonia.profiler.CacheTraceProfiler import get_workload_feature_dict_from_cache_trace


class NumpyEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ndarray):
            return obj.tolist()
        elif isinstance(obj, int64):
            return int(obj)
        return JSONEncoder.default(self, obj)


def process_block_req(
        cache_req_df: DataFrame, 
        lba_size_byte: int, 
        block_size_byte: int
) -> list:

    block_req_arr = []

    if not cache_req_df["op"].str.contains('w').any():
        cur_cache_req_df = cache_req_df
        cur_op = 'r'
    else:
        cur_cache_req_df = cache_req_df[cache_req_df["op"] == 'w']
        cur_op = 'w'


    # handle the misalignment possible in the first block accessed
    first_cache_req = cur_cache_req_df.iloc[0]
    iat_us = first_cache_req["iat"]
    prev_key = first_cache_req["key"]
    rear_misalign_byte = first_cache_req["rear_misalign"]
    req_start_byte = (first_cache_req["key"] * block_size_byte) + first_cache_req["front_misalign"]
    req_size_byte = block_size_byte - first_cache_req["front_misalign"]


    for _, row in cur_cache_req_df.iloc[1:].iterrows():
        cur_key = row["key"]
        if cur_key - 1 == prev_key:
            req_size_byte += block_size_byte
        else:
            block_req_arr.append({
                "iat": iat_us,
                "lba": int(req_start_byte/lba_size_byte),
                "size": req_size_byte,
                "op": cur_op
            })
            req_start_byte = cur_key * block_size_byte
            req_size_byte = block_size_byte
        rear_misalign_byte = row["rear_misalign"]
        prev_key = cur_key
    
    block_req_arr.append({
        "iat": iat_us,
        "lba": int(req_start_byte/lba_size_byte),
        "size": int(req_size_byte - rear_misalign_byte),
        "op": cur_op
    })
    return block_req_arr



def generate_block_trace(
        cache_trace_path: Path, 
        block_trace_path: Path,
        lba_size_byte: int = 512, 
        block_size_byte: int = 4096
) -> None:
    cur_ts = 0
    cache_trace_df = read_csv(cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])
    with block_trace_path.open("w+") as block_trace_handle:
        for _, group_df in cache_trace_df.groupby(by=['i']):
            sorted_group_df = group_df.sort_values(by=["key"])
            block_req_arr = process_block_req(sorted_group_df, lba_size_byte, block_size_byte)

            for cur_block_req in block_req_arr:
                cur_ts += int(cur_block_req["iat"])
                assert int(cur_block_req["size"]) >= lba_size_byte, "Size too small {}.".format(int(cur_block_req["size"]))
                block_trace_handle.write("{},{},{},{}\n".format(cur_ts, int(cur_block_req["lba"]), cur_block_req["op"], int(cur_block_req["size"])))
    


    # print("Block trace generated, now comparing features.")
    # cache_workload_features = get_workload_feature_dict_from_cache_trace(read_csv(cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"]))
    # block_workload_features = get_workload_feature_dict_from_block_trace(block_trace_path)

    # print(dumps(cache_workload_features, indent=2, cls=NumpyEncoder))
    # print(dumps(block_workload_features, indent=2, cls=NumpyEncoder))
    # for feature_name in cache_workload_features:
    #     print("{} -> {}, {}".format(feature_name, cache_workload_features[feature_name], block_workload_features[feature_name]))
    #     assert cache_workload_features[feature_name] == block_workload_features[feature_name]



def main():
    parser = ArgumentParser(description="Create block trace from cache trace.")
    parser.add_argument("-t",
                            "--type",
                            type=str,
                            default="iat",
                            help="The sample type.")
    parser.add_argument("-w",
                            "--workload",
                            type=str,
                            help="Name of the workload.",
                            required=True)
    parser.add_argument("-r",
                            "--rate",
                            type=int,
                            help="The sampling rate.",
                            required=True)
    parser.add_argument("-b",
                            "--bits",
                            type=int,
                            help="Number of lower order bits ignored during sampling.",
                            required=True)
    parser.add_argument("-s",
                            "--seed",
                            type=int,
                            help="Random seed used during sampling.",
                            required=True)
    args = parser.parse_args()

    config = BaseConfig()
    sample_cache_trace_path_list = config.get_all_sample_cache_traces(args.type, args.workload)
    for cache_trace_path in sample_cache_trace_path_list:
        if ".rd" in cache_trace_path.name:
            continue 
        split_cache_file_name = cache_trace_path.stem.split('_')
        rate, bits, seed = int(split_cache_file_name[0]), int(split_cache_file_name[1]), int(split_cache_file_name[2])

        if rate != args.rate:
            continue 

        if bits != args.bits:
            continue 

        if seed != args.seed:
            continue 

        block_trace_path = config.get_sample_block_trace_path(args.type, args.workload, rate, bits, seed)
        if not block_trace_path.exists():
            print("Generating block trace {} from cache trace {}.".format(block_trace_path, cache_trace_path))
            block_trace_path.parent.mkdir(exist_ok=True, parents=True)
            generate_block_trace(cache_trace_path, block_trace_path)
        else:
            print("Already exists block trace {} from cache trace {}.".format(block_trace_path, cache_trace_path))


if __name__ == "__main__":
    main()