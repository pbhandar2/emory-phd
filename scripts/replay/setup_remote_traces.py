from pathlib import Path 
from pandas import read_csv, DataFrame
from os import environ 

from cydonia.util.S3Client import S3Client
from keyuri.config.BaseConfig import BaseConfig

REPLAY_EXPERIMENT_FILE = Path("files/experiments.csv")

def load_cache_trace(cache_trace_path: Path) -> DataFrame:
    """Load a cache trace file into a DataFrame.

    Args:
        cache_trace_path: Path to cache trace.

    Returns:
        cache_trace_df: DataFrame with cache trace.
    """
    return read_csv(cache_trace_path, 
                        names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])

def get_block_req_arr(
        cache_req_df: DataFrame, 
        lba_size_byte: int, 
        block_size_byte: int
) -> list:
    """Get block requests from a set of cache requests originating from the
    same source block request.

    Args:
        cache_req_df: DataFrame containing a set of cache requests.
        lba_size_byte: Size of an LBA in bytes. 
        block_size_byte: Size of a cache block in bytes.
    
    Returns:
        block_req_arr: List of dictionary with attributes of each block request.
    """
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
    assert all([req["size"]>0 for req in block_req_arr]), "All sizes not greater than 0, found {}.".format(block_req_arr)
    return block_req_arr
        

def generate_block_trace(
        cache_trace_path: Path, 
        block_trace_path: Path,
        lba_size_byte: int = 512, 
        block_size_byte: int = 4096
) -> None:
    """Generate block trace from cache trace.

    Args:
        cache_trace_path: Path of the cache trace.
        block_trace_path: Path of the block trace to be generated.
        lba_size_byte: Size of an LBA in bytes. 
        block_size_byte: Size of a cache block in bytes. 
    """
    cur_ts = 0
    cache_trace_df = load_cache_trace(cache_trace_path)
    with block_trace_path.open("w+") as block_trace_handle:
        for _, group_df in cache_trace_df.groupby(by=['i']):
            sorted_group_df = group_df.sort_values(by=["key"])
            block_req_arr = get_block_req_arr(sorted_group_df, lba_size_byte, block_size_byte)

            for cur_block_req in block_req_arr:
                cur_ts += int(cur_block_req["iat"])
                assert int(cur_block_req["size"]) >= lba_size_byte, "Size too small {}.".format(int(cur_block_req["size"]))
                block_trace_handle.write("{},{},{},{}\n".format(cur_ts, int(cur_block_req["lba"]), cur_block_req["op"], int(cur_block_req["size"])))


def main():
    base_config = BaseConfig() 
    s3_client = S3Client(environ["AWS_KEY"], environ["AWS_SECRET"], environ["AWS_BUCKET"])
    df = read_csv(REPLAY_EXPERIMENT_FILE)
    for row_index, cur_row in df.iterrows():
        key = cur_row["traceKey"]

        if s3_client.check_prefix_exist(key):
            print(key, " exists!")
            continue 
            
        print(key, " does not exist!")
        path_key = Path(key)
        split_key = path_key.name.split("-")

        if len(split_key) == 1:
            block_trace_path = base_config._block_trace_dir_path.joinpath(split_key[0])
        elif len(split_key) == 2:
            workload_name = split_key[0]
            sample_set_name = path_key.parent.name.split("-").pop()
            sample_cache_trace_dir_path = base_config.get_sample_cache_trace_dir_path(sample_set_name, workload_name)
            cache_trace_path = sample_cache_trace_dir_path.joinpath(split_key[1])
            block_trace_path = Path("/tmp/block.csv")
            generate_block_trace(cache_trace_path, block_trace_path)   
        elif len(split_key) == 4:
            sample_set_name = path_key.parent.name.split("-").pop()
            compound_workload_set_name = base_config.get_compound_workload_set_name(sample_set_name)
            temp_base_config = BaseConfig(workload_set_name=compound_workload_set_name)
            cache_trace_path = temp_base_config._post_process_cache_trace_dir_path.joinpath("/".join(split_key))
            block_trace_path = Path("/tmp/block.csv")
            generate_block_trace(cache_trace_path, block_trace_path)   
        else:
            raise ValueError("Invalid key ", key)

        print("Uploading {} to {}".format(block_trace_path, key))
        s3_client.upload_s3_obj(key, str(block_trace_path.absolute()))

if __name__ == "__main__":
    main()

