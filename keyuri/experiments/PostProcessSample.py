from json import load 
from pathlib import Path 
from copy import deepcopy
from time import perf_counter_ns
from pandas import DataFrame, read_csv, concat 


from cydonia.profiler.BAFM import BAFM, BAFMOutput
from cydonia.profiler.WorkloadStats import WorkloadStats

from keyuri.config.BaseConfig import BaseConfig


class PostProcessSample:
    def __init__(
            self,
            workload_name: str,
            sample_type: str, 
            metric_name: str, 
            rate: int,
            bits: int,
            seed: int,
            algo_bits: int,
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

    
    def run(self, num_iter: int):
        cur_workload_stat = deepcopy(self._sample_workload_stat)
        bafm = BAFM(self._algo_bits)
        bafm.load_block_access_file(self._access_feature_file_path)
        if self._output_file_path.exists():
            bafm_output = BAFMOutput(self._output_file_path)
            num_lines = bafm_output.num_blocks_removed()

            if num_lines >= num_iter:
                print("{} blocks already removed, asked to remove {} blocks.".format(num_lines, num_iter))
                return 
            else:
                num_iter -= num_lines
            
            cur_workload_stat = bafm.update_state(self._output_file_path, self._full_workload_stat)
        
        bafm.remove_n_blocks(self._full_workload_stat,
                                cur_workload_stat,
                                self._metric_name,
                                num_iter,
                                self._output_file_path)