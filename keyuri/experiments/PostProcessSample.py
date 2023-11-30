from pathlib import Path 
from json import load 
from copy import deepcopy
from time import perf_counter_ns
from pandas import DataFrame, read_csv, concat 

from cydonia.profiler.CacheTraceProfiler import get_workload_feature_dict_from_cache_trace, validate_cache_trace, load_cache_trace
from cydonia.profiler.BlockAccessFeatureMap import BlockAccessFeatureMap
from cydonia.blksample.SamplePP import SamplePP

from keyuri.config.BaseConfig import BaseConfig


class PostProcessSample:
    def __init__(
            self,
            workload_name: str,
            workload_type: str = "cp"
    ) -> None:
        self._workload_name = workload_name 
        self._workload_type = workload_type 
        self._config = BaseConfig() 
        self._block_trace_path = self._config.get_block_trace_path(self._workload_name)
        self._full_workload_feature_dict = {}
        with self._config.get_cache_features_path(self._workload_name).open("r") as cache_feature_file_handle:
            self._full_workload_feature_dict = load(cache_feature_file_handle)
    

    def post_process(
            self,
            rate: int,
            bits: int,
            seed: int,
            algo_bits: int,
            sample_type: str = "iat",
            algo_type: str = "best"
    ) -> None:
        sample_cache_trace_path = self._config.get_sample_cache_trace_path(sample_type,
                                                                            self._workload_name,
                                                                            rate,
                                                                            bits,
                                                                            seed)

        sample_block_trace_path = self._config.get_sample_block_trace_path(sample_type,
                                                                            self._workload_name,
                                                                            rate,
                                                                            bits,
                                                                            seed)
        
        output_log_path = self._config.get_algo_output_file_path(algo_type,
                                                                    sample_type,
                                                                    self._workload_name,
                                                                    rate,
                                                                    bits,
                                                                    seed,
                                                                    algo_bits)
        output_log_path.parent.mkdir(exist_ok=True, parents=True)
        if output_log_path.exists():
            output_log_path.unlink()

        validate_cache_trace(sample_block_trace_path, sample_cache_trace_path)
        sample_cache_trace_feature_dict = get_workload_feature_dict_from_cache_trace(load_cache_trace(sample_cache_trace_path))

        block_size_byte = (2**algo_bits) * 4096
        feature_map = BlockAccessFeatureMap(block_size_byte=block_size_byte)
        feature_map.load(sample_block_trace_path)

        sample_pp = SamplePP(sample_cache_trace_feature_dict, self._full_workload_feature_dict, feature_map)

        start_time = perf_counter_ns()
        if algo_type == "best":
            block_removed = sample_pp.remove_best_block()
        else:
            block_removed = sample_pp.remove_next_block()

        while block_removed >= 0:
            cur_err_dict = deepcopy(sample_pp.get_err_dict())
            cur_err_dict["time"] = perf_counter_ns() - start_time 
            cur_err_dict["addr"] = block_removed
            self.write_err_dict_to_file(cur_err_dict, output_log_path)
            print(cur_err_dict)
            start_time = perf_counter_ns()
            if algo_type == "best":
                block_removed = sample_pp.remove_best_block()
            else:
                block_removed = sample_pp.remove_next_block()

    
    @staticmethod
    def write_err_dict_to_file(err_dict: dict, output_file_path: Path):
        err_df = DataFrame([err_dict])
        if output_file_path.exists():
            cur_df = read_csv(output_file_path)
            new_df = concat([cur_df, err_df], ignore_index=True)
            new_df.to_csv(output_file_path, index=False)
        else:
            err_df.to_csv(output_file_path, index=False)