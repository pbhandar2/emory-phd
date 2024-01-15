from json import load 
from pathlib import Path 
from copy import deepcopy
from time import perf_counter_ns
from pandas import DataFrame, read_csv, concat 


from cydonia.profiler.BAFM import BAFM, BAFMOutput
from cydonia.profiler.WorkloadStats import WorkloadStats
from cydonia.profiler.CacheTrace import CacheTraceReader

from keyuri.config.BaseConfig import BaseConfig


class PPTargetRate:
    def __init__(
            self,
            workload_name: str,
            sample_type: str, 
            metric_name: str, 
            rate: int,
            bits: int,
            seed: int,
            algo_bits: int,
            block_count: int, 
            workload_type: str = "cp"
    ) -> None:
        self._workload_name = workload_name 
        self._workload_type = workload_type 
        self._sample_type = sample_type 
        self._metric_name = metric_name
        self._rate = rate 
        self._bits = bits 
        self._seed = seed 
        self._algo_bits = algo_bits
        self._config = BaseConfig() 
        self._block_count = block_count

        self._sample_cache_trace_path = self._config.get_sample_cache_trace_path(self._sample_type,
                                                                                    self._workload_name,
                                                                                    int(100*self._rate),
                                                                                    self._bits,
                                                                                    self._seed)
        
        cache_trace_reader = CacheTraceReader(self._sample_cache_trace_path)
        self._sample_block_set = cache_trace_reader.get_unscaled_unique_block_addr_set()

        print("There are {} blocks in sample from full trace with {} blocks.".format(len(self._sample_block_set), self._block_count))

        with self._config.get_cache_features_path(self._workload_name).open("r") as cache_feature_file_handle:
            self._full_workload_feature_dict = load(cache_feature_file_handle)

        with self._config.get_sample_cache_features_path(self._sample_type, 
                                                            self._workload_name,
                                                            int(100*self._rate),
                                                            self._bits,
                                                            self._seed).open("r") as sample_cache_feature_file_handle:
            self._sample_workload_feature_dict = load(sample_cache_feature_file_handle)
        
        self._full_workload_stat = WorkloadStats()
        self._full_workload_stat.load_dict(self._full_workload_feature_dict)

        self._sample_workload_stat = WorkloadStats()
        self._sample_workload_stat.load_dict(self._sample_workload_feature_dict)
        
        self._access_feature_file_path = self._config.get_sample_access_feature_file_path(self._sample_type,
                                                                                            self._workload_name,
                                                                                            int(100*self._rate),
                                                                                            self._bits,
                                                                                            self._seed)

        self._output_file_path = self._config.get_sample_post_process_output_file_path(self._sample_type,
                                                                                        self._workload_name,
                                                                                        self._metric_name,
                                                                                        self._algo_bits,
                                                                                        int(100*self._rate),
                                                                                        self._bits,
                                                                                        self._seed)
        
        self._output_file_path.parent.mkdir(exist_ok=True, parents=True)

        print("Post Processing Sample")
        print(self._access_feature_file_path)
        print(self._output_file_path)
        print(self._full_workload_feature_dict)
        print("There are {} blocks in the full workload.".format(self._block_count))

    
    def run(
            self, 
            target_sampling_rate: float
    ) -> None:
        cur_workload_stat = deepcopy(self._sample_workload_stat)

        bafm = BAFM(self._algo_bits)
        bafm.load_block_access_file(self._access_feature_file_path)

        if self._output_file_path.exists():
            bafm_output = BAFMOutput(self._output_file_path)
            for addr in bafm_output.get_addr_removed():
                for unscaled_addr in CacheTraceReader.get_blk_addr_arr(addr, self._bits):
                    if unscaled_addr in self._sample_block_set:
                        self._sample_block_set.remove(unscaled_addr)
            cur_sampling_rate = len(self._sample_block_set)/self._block_count

            if cur_sampling_rate <= target_sampling_rate:
                print("target sampling rate {} <= current sampling rate {}.".format(target_sampling_rate, cur_sampling_rate))
                return 
            
            cur_workload_stat = bafm.update_state(self._output_file_path, self._full_workload_stat)

        
        bafm.target_sampling_rate(self._full_workload_stat,
                                    cur_workload_stat,
                                    self._metric_name,
                                    self._block_count,
                                    self._sample_block_set,
                                    target_sampling_rate,
                                    self._output_file_path)