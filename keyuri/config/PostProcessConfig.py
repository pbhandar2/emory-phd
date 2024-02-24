from pathlib import Path 
from pandas import read_csv 

from keyuri.config.BaseConfig import BaseConfig


def validate_post_process_output_file(output_file, target_sampling_rate):
    df = read_csv(output_file)
    return len(df[df["rate"] <= target_sampling_rate]) > 0 


def get_num_iter_post_processing(output_file, target_sampling_rate):
    df = read_csv(output_file)
    return len(df[df["rate"] >= target_sampling_rate])


def get_min_num_iter_post_processing(output_file, target_sampling_rate):
    df = read_csv(output_file)
    filter_df = df[df["rate"] >= target_sampling_rate]
    min_index = filter_df["mean"].idxmin() 
    return min_index+ 1 


class PostProcessFiles:
    def __init__(self, config: BaseConfig):
        self._config = config 


    def get_files_for_hit_rate_err_experiment(self,
                                                sample_set: str ,
                                                workload: str ,
                                                algo_metric: str,
                                                algo_bits: int,
                                                sampling_rate: float,
                                                sampling_bits: int,
                                                sampling_seed: int):

        file_dict = {}

        # the sample file 
        sample_file_path = self._config.get_sample_cache_trace_path(sample_set,
                                                                    workload,
                                                                    int(100*sampling_rate),
                                                                    sampling_bits,
                                                                    sampling_seed)
        if sample_file_path.exists():
            file_dict["sample_file_path"] = sample_file_path
        else:
            return file_dict, False 
        
        # RD hist of fill trace 
        full_rd_hist_path = self._config.get_rd_hist_file_path(workload)
        if full_rd_hist_path.exists():
            file_dict["full_rd_hist_file_path"] = full_rd_hist_path
        else:
            return file_dict, False 

        # the output file from post processing 
        post_process_output_file_path = self._config.get_sample_post_process_output_file_path(sample_set,
                                                                                                workload,
                                                                                                algo_metric,
                                                                                                algo_bits,
                                                                                                int(100*sampling_rate),
                                                                                                sampling_bits,
                                                                                                sampling_seed)
        
        if post_process_output_file_path.exists():
            file_dict["post_process_output_file_path"] = post_process_output_file_path
        else:
            print("File not found {}".format(post_process_output_file_path))
            return file_dict, False 
        
        return file_dict, True 