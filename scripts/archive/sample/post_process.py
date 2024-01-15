"""This script runs post-processing algorithms on sample block traces. 

Example:
    python3 post_process.py w18 -r 1 5 10 20 40 80 -s 42 43 44 -b 12 10 8 4 2 0 -ba 20 18 16 14 12 10 8 4 2 0
    python3 post_process.py w92 -r 1 5 -s 42 -b 12 10 -ba 12 10 
    python3 post_process.py w92 -r 10 -s 42 -b 12 10 -ba 12 10 
    python3 post_process.py w92 -r 20 -s 42 -b 12 10 -ba 12 10 
"""

from pathlib import Path 
from pandas import DataFrame
from itertools import product
from time import perf_counter_ns
from argparse import ArgumentParser
from json import dump, dumps, JSONEncoder
from numpy import integer, floating, ndarray

from cydonia.sample.Sample import create_sample_trace
from keyuri.config.Config import GlobalConfig, SampleExperimentConfig
from cydonia.sample.BlkSample import load_blk_trace, get_workload_stat_dict, blk_unsample, get_unique_blk_addr_set, get_blk_addr_arr, get_percent_error_dict


"""This class handles numpy integers, floats and array in JSON when
using functions like dump, dumps. A lot of our data might contains 
these data types the json package does not support. 
"""
class NpEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, integer):
            return int(obj)
        if isinstance(obj, floating):
            return float(obj)
        if isinstance(obj, ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
    

def get_post_process_files(
    workload_type: str,
    workload_name: str, 
    sample_type: str, 
    rate: int,
    seed: int,
    bits: int,
    post_process_algorithm: str, 
    global_config: GlobalConfig = GlobalConfig(),
    sample_config: SampleExperimentConfig = SampleExperimentConfig()
) -> tuple:
    """Get the list of path of files that we need for post-processing. 

    Args:
        workload_type: The workload type. 
        workload_name: The workload name. 
        sample_type: The sampling type. 
        rate: The specified sampling rate. 
        seed: The random seed used during sampling.
        bits: The number of lower order bits ignored during sampling.
        post_process_algorithm: The type of post-processing algorithm. 
        global_config: The global configuration for all experiments. 
        sample_config: The configuration for sampling experiments. 
    
    Returns:
        post_process_file_tuple: A tuple of path to the current sample trace path, error file per iteration, 
                                    metadata file and new sample created after post-processing.  
    """
    sample_trace_path = sample_config.get_sample_trace_path(sample_type, 
                                                                workload_type, 
                                                                workload_name,
                                                                rate, 
                                                                bits, 
                                                                seed, 
                                                                global_config=global_config)
    
    if not sample_trace_path.exists():
        sample_trace_path = None

    per_iteration_error_file_path = sample_config.get_per_iteration_error_file_path(post_process_algorithm,
                                                                                        sample_type, 
                                                                                        workload_type, 
                                                                                        workload_name,
                                                                                        rate, 
                                                                                        bits, 
                                                                                        seed, 
                                                                                        global_config=global_config)
    per_iteration_error_file_path.parent.mkdir(exist_ok=True, parents=True)

    metadata_file_path = sample_config.get_post_process_metadata_file_path(post_process_algorithm,
                                                                            sample_type, 
                                                                            workload_type, 
                                                                            workload_name,
                                                                            rate, 
                                                                            bits, 
                                                                            seed, 
                                                                            global_config=global_config)
    metadata_file_path.parent.mkdir(exist_ok=True, parents=True)

    postprocess_sample_file_path = sample_config.get_post_process_sample_file_path(post_process_algorithm,
                                                                                    sample_type, 
                                                                                    workload_type, 
                                                                                    workload_name,
                                                                                    rate, 
                                                                                    bits, 
                                                                                    seed, 
                                                                                    global_config=global_config)
    postprocess_sample_file_path.parent.mkdir(exist_ok=True, parents=True)
    return sample_trace_path, per_iteration_error_file_path, metadata_file_path, postprocess_sample_file_path


def post_process(
    workload_type: str,
    workload_name: str, 
    sample_type: str, 
    rate_arr: list,
    seed_arr: list,
    bits_arr: list,
    bits_algo_arr: list, 
    post_process_algorithm: str, 
    global_config: GlobalConfig = GlobalConfig(),
    sample_config: SampleExperimentConfig = SampleExperimentConfig()
) -> None:
    """Run post process on samlpes based on the provided parameters. 

    Args:
        workload_type: The workload type. 
        workload_name: The workload name. 
        sample_type: The sampling type. 
        rate_arr: Array of specified sampling rate. 
        seed_arr: Array of random seed used during sampling.
        bits_arr: Array of the number of lower order bits ignored during sampling.
        bits_algo_arr: Array of the number of lower order bits ignored during post-processing. 
        post_process_algorithm: The type of post-processing algorithm. 
        global_config: The global configuration for all experiments. 
        sample_config: The configuration for sampling experiments. 
    """
    block_trace_path = global_config.get_block_trace_path(workload_type, workload_name)
    assert block_trace_path.exists(), "Block trace path {} does not exist.".format(block_trace_path)

    full_df = DataFrame([])
    full_blk_unique_lba_set = set()
    full_workload_stat_dict = {}
    for rate, seed, bits, algo_bits in product(rate_arr, seed_arr, bits_arr, bits_algo_arr):
        post_process_name = "{}-{}".format(post_process_algorithm, algo_bits)
        post_process_file_tuple = get_post_process_files(workload_type,
                                                            workload_name,
                                                            sample_type,
                                                            rate,
                                                            seed,
                                                            bits,
                                                            post_process_name,
                                                            global_config=global_config,
                                                            sample_config=sample_config)
        
        metadata_file_path = post_process_file_tuple[2]
        if post_process_file_tuple[0] is None:
            print("Sample not found {}.".format(post_process_file_tuple[0]))
            continue 
        elif metadata_file_path.exists():
            print("Metadata file path {} already exists.".format(metadata_file_path))
            continue 

        if len(full_blk_unique_lba_set) == 0:
            # delayed computation of expensive variables until we know there will be something to do 
            # even though we only have to do it once 
            full_df = load_blk_trace(block_trace_path)
            full_blk_unique_lba_set = get_unique_blk_addr_set(full_df)
            full_workload_stat_dict = get_workload_stat_dict(full_df)
            print("Full workload data loaded.")

        post_process_start_ns = perf_counter_ns()
        print("Running {},{},{} for {} using {}.".format(rate, seed, bits, block_trace_path, post_process_algorithm))
        sample_df = load_blk_trace(post_process_file_tuple[0])
        sample_workload_stat_dict = get_workload_stat_dict(sample_df)
        sample_percent_error_dict = get_percent_error_dict(full_workload_stat_dict, sample_workload_stat_dict)

        blk_unsample_start_time = perf_counter_ns()
        per_iteration_error_df = blk_unsample(sample_df, full_workload_stat_dict, num_lower_order_bits_ignored=algo_bits)
        blk_unsample_end_time = perf_counter_ns()
        print(per_iteration_error_df)

        per_iteration_error_df.to_csv(post_process_file_tuple[1])
        best_sample_row = per_iteration_error_df.sort_values(by=["mean", "std"]).iloc[0] 

        sample_blk_unique_lba_set = get_unique_blk_addr_set(sample_df)
        assert sample_blk_unique_lba_set.issubset(full_blk_unique_lba_set), \
            "The set of unique blocks in sample is not a subset of of set of unique blocks of full trace."

        regions_to_be_removed_list = []
        for _, per_iteration_row in per_iteration_error_df.iterrows():
            if int(per_iteration_row["region"]) == int(best_sample_row["region"]):
                regions_to_be_removed_list.append(int(per_iteration_row["region"]))
                break 
            else:
                regions_to_be_removed_list.append(int(per_iteration_row["region"]))
        else:
            raise ValueError("Didn't find the the best sample row.")

        blk_addr_to_be_removed_list = []
        for region_to_be_removed in regions_to_be_removed_list:
            blk_addr_list = get_blk_addr_arr(region_to_be_removed, algo_bits)
            for blk_addr in blk_addr_list:
                if blk_addr in sample_blk_unique_lba_set:
                    blk_addr_to_be_removed_list.append(blk_addr)
        
        print("We removed {} out of {} blocks.".format(len(set(blk_addr_to_be_removed_list)), len(sample_blk_unique_lba_set)))
        new_sample_unique_lba_set = sample_blk_unique_lba_set.difference(set(blk_addr_to_be_removed_list))
        print("New sample will have {} blocks.".format(len(new_sample_unique_lba_set)))

        start_eff_rate = 100.0 * len(sample_blk_unique_lba_set)/len(full_blk_unique_lba_set)
        new_eff_rate = 100.0 * (len(sample_blk_unique_lba_set) - len(blk_addr_to_be_removed_list))/len(full_blk_unique_lba_set)
        sample_blk_addr_dict = dict.fromkeys(list(new_sample_unique_lba_set), 1)
        create_sample_trace(sample_df, sample_blk_addr_dict, post_process_file_tuple[3])
        new_sample_created_time = perf_counter_ns()

        new_sample_df = load_blk_trace(post_process_file_tuple[3])
        new_sample_workload_stat_dict = get_workload_stat_dict(new_sample_df)
        new_sample_percent_error_dict = get_percent_error_dict(full_workload_stat_dict, new_sample_workload_stat_dict)
        new_unique_lba_set = get_unique_blk_addr_set(new_sample_df)
        print("Create post process sample at {}.".format(post_process_file_tuple[3]))

        for key_name in new_sample_percent_error_dict:
            assert new_sample_percent_error_dict[key_name] == best_sample_row[key_name], \
                "The key {} did not match for new error dict {} and compute dict {}.".format(key_name, new_sample_percent_error_dict, best_sample_row)

        end_time = perf_counter_ns()
        metadata_dict = {
            "full_lba_count": len(full_blk_unique_lba_set),
            "sample_lba_count": len(sample_blk_unique_lba_set),
            "postprocess_lba_count": len(new_unique_lba_set),
            "specified_rate": rate,
            "sample_eff_rate": start_eff_rate,
            "postprocess_eff_rate": new_eff_rate,
            "full_line_count": len(full_df),
            "sample_line_count": len(sample_df),
            "postprocess_line_count": len(new_sample_df),
            "full_file_size": block_trace_path.stat().st_size,
            "sample_file_size": post_process_file_tuple[0].stat().st_size,
            "postprocess_file_size": post_process_file_tuple[3].stat().st_size,
            "full_stat": full_workload_stat_dict,
            "sample_stat": sample_workload_stat_dict,
            "postprocess_stat": new_sample_workload_stat_dict,
            "sample_percent_err": sample_percent_error_dict,
            "postprocess_percent_err": new_sample_percent_error_dict,
            "runtime": end_time - post_process_start_ns,
            "blkunsample_runtime": blk_unsample_end_time - blk_unsample_start_time,
            "sample_creation_runtime": new_sample_created_time - blk_unsample_end_time,
            "load_trace_runtime": blk_unsample_start_time - post_process_start_ns
        }
        print(dumps(metadata_dict, indent=2, cls=NpEncoder))
        with post_process_file_tuple[2].open("w+") as metadata_handle:
            dump(metadata_dict, metadata_handle, indent=2, cls=NpEncoder)


def main():
    global_config, sample_config = GlobalConfig(), SampleExperimentConfig()

    parser = ArgumentParser(description="Post process sample block traces.")
    parser.add_argument("workload_name", 
                            type=str, 
                            help="The name of the workload.")

    parser.add_argument("-r",
                            "--rate", 
                            nargs='+', 
                            type=int, 
                            help="Sampling rate in percentage.", 
                            required=True)

    parser.add_argument("-s",
                            "--seed", 
                            nargs='+', 
                            type=int, 
                            help="Random seed of the sample.", 
                            required=True)

    parser.add_argument("-b",
                            "--bits", 
                            nargs='+', 
                            type=int, 
                            help="Number of lower order bits of addresses ignored.", 
                            required=True)
    
    parser.add_argument("-ba",
                            "--bits_algo",
                            nargs='+', 
                            type=int, 
                            help="Number of lower order bits ignored by the post processing algorithm.",
                            required=True)

    parser.add_argument("--sample_type", 
                            type=str, 
                            default="iat", 
                            help="The sample type to be evaluated.")
    
    parser.add_argument("--workload_type", 
                            type=str, 
                            default="cp", 
                            help="The type of workload.")
    
    parser.add_argument("--source_dir_path", 
                            type=Path, 
                            default=global_config.source_dir_path, 
                            help="Source directory of all data.")

    parser.add_argument("--algo", 
                            type=str, 
                            default="reduce", 
                            help="Algorithm used to postprocess the sample.")

    args = parser.parse_args()

    if args.source_dir_path != global_config.source_dir_path:
        global_config.update_source_dir(args.source_dir_path) 
    
    post_process(
        args.workload_type,
        args.workload_name,
        args.sample_type,
        args.rate,
        args.seed,
        args.bits,
        args.bits_algo,
        args.algo,
        global_config=global_config,
        sample_config=sample_config
    )


if __name__ == "__main__":
    main()
    