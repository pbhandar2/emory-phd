from argparse import ArgumentParser
from pathlib import Path
from json import load 
from pandas import read_csv, DataFrame, concat 

from keyuri.config.BaseConfig import BaseConfig

from cydonia.profiler.CacheTrace import CacheTraceReader


def sample_features(
        full_cache_feature_path: Path, 
        sample_cache_feature_path_list: list, 
        sample_cache_trace_path_list: list,
        sample_feature_output_path: Path,
        config: BaseConfig
) -> None:
    with full_cache_feature_path.open("r") as handle:
        full_cache_feature_dict = load(handle)
    full_cache_total_req = full_cache_feature_dict["block_read_count"] + full_cache_feature_dict["block_write_count"]

    output_df = None 
    if sample_feature_output_path.exists():
        output_df = read_csv(sample_feature_output_path)

    data_dict_arr = []
    for sample_cache_feature_path in sample_cache_feature_path_list:
        with sample_cache_feature_path.open("r") as handle:
            sample_cache_feature_dict = load(handle)
        
        rate, bits, seed = config.get_sample_file_info(sample_cache_feature_path)
        if output_df is not None:
            output_row = output_df[(output_df["rate"] == rate) & (output_df["bits"] == bits) & (output_df["seed"] == seed)]
            assert len(output_row) <= 1, "multiple rows with sample rate bits seed in output df"
            if len(output_df) > 0:
                print("already computed for file {}.".format(sample_cache_feature_path))
                continue 
        
        cur_sample_cache_trace_path = None 
        for sample_cache_trace_path in sample_cache_trace_path_list:
            if sample_cache_feature_path.name == sample_cache_trace_path.name:
                cur_sample_cache_trace_path = sample_cache_trace_path
                break 
        
        print("evaluating sample split ratio of {}".format(sample_cache_feature_path))
        assert cur_sample_cache_trace_path is not None, "Did not find cache trace for feature file {}.".format(sample_cache_feature_path)
        cache_trace_reader = CacheTraceReader(cur_sample_cache_trace_path)
        mean_sample_split = cache_trace_reader.get_mean_sample_split()

        sample_cache_total_req = sample_cache_feature_dict["block_read_count"] + sample_cache_feature_dict["block_write_count"]

        data_dict = {}
        data_dict["mean_sample_split"] = mean_sample_split
        data_dict["req_ratio"] = sample_cache_total_req/full_cache_total_req
        data_dict["rate"], data_dict["bits"], data_dict["seed"] = rate, bits, seed
        data_dict_arr.append(data_dict)

        print(data_dict)
    
    sample_feature_output_path.parent.mkdir(exist_ok=True, parents=True)
    new_df = DataFrame(data_dict_arr)
    if output_df is not None:
        cum_df = concat([new_df, output_df])
        cum_df.to_csv(sample_feature_output_path, index=False)
    else:
        new_df.to_csv(sample_feature_output_path, index=False)
        


        

def main():
    parser = ArgumentParser(description="Generate sampling features from the sample.")

    parser.add_argument("workload", type=str, help="Name of workload.")

    parser.add_argument("--type",
                            "-t", 
                            type=str, 
                            default="basic",
                            help="The sample type.")

    args = parser.parse_args()

    config = BaseConfig()

    full_cache_feature_path = config.get_cache_features_path(args.workload)
    sample_cache_feature_path_list = config.get_all_sample_cache_features(args.type, args.workload)
    sample_cache_trace_path_list = config.get_all_sample_cache_traces(args.type, args.workload)
    sample_feature_path = config.get_sample_feature_file_path(args.type, args.workload)

    

    sample_features(full_cache_feature_path, sample_cache_feature_path_list, sample_cache_trace_path_list, sample_feature_path, config)



    


if __name__ == "__main__":
    main()