"""This script generates features from the cache traces. 

It uses the multiprocessing module for parallel processing, and BlockAccessTraceProfiler 
from cydonia package for profiling the samples. 

Typical usage example:
    profile_cache_traces = ProfileCacheTraces()
    profile_cache_traces.profile()
"""

from itertools import product 
from pathlib import Path 
from multiprocessing import cpu_count, Pool

from cydonia.profiler.BlockAccessTraceProfiler import BlockAccessTraceProfiler
from cydonia.profiler.CacheTraceProfiler import CacheTraceProfiler

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


class ProfileCacheTraces:
    def __init__(
            self,
            global_config: GlobalConfig = GlobalConfig(),
            sample_config: SampleExperimentConfig = SampleExperimentConfig()
    ) -> None:
        """This class profiles samples in parallel using multiprocessing.
        
        Attributes:
            _global_config: Global configuration of experiments. 
            _sample_config: Configuration of sampling experiments. 
        """
        self._global_config = global_config
        self._sample_config = sample_config


    def profile_process(
            self, 
            param_arr: list
    ) -> None:
        """The function used by a process to profile a set of traces.
        
        Args:
            param_arr: Array of parameters for profiling.
        """
        for profile_params in param_arr:
            print("Profiling {}".format(profile_params))
            profiler = CacheTraceProfiler(str(profile_params["trace_path"].absolute()))
            profile_params["feature_path"].parent.mkdir(exist_ok=True, parents=True)
            profiler.create_rd_hist_file(profile_params["feature_path"])
            print("Profiling {} completed.")


    def profile(
            self,
            workload_name: str, 
            workload_type: str = "cp",
            batch_size: int = 4,
            sample_type: str = "iat"
    ) -> None:
        """Profile samples for a given workload.

        Args:
            workload_name: Name of the workload.
            workload_type: Type of workload. 
            batch_size: Number of sampling processers to start at once. 
            sample_type: The type of sampling technique used. 
        """
        profile_param_arr = []
        seed_arr, bits_arr, rate_arr = self._sample_config.seed_arr, self._sample_config.bits_arr, self._sample_config.rate_arr
        for seed, bits, rate in product(seed_arr, bits_arr, rate_arr):
            trace_path = self._sample_config.get_sample_cache_trace_path(sample_type, workload_type, workload_name, rate, bits, seed, global_config=self._global_config)
            if not trace_path.exists():
                continue 

            feature_file_path = self._sample_config.get_rd_hist_file_path(sample_type, workload_type, workload_name, rate, bits, seed, global_config=self._global_config)
            if feature_file_path.exists():
                assert feature_file_path.is_file(), "Feauture file {} is not a file.".format(feature_file_path)
                continue 

            profile_param = {
                "trace_path": trace_path,
                "feature_path": feature_file_path
            }
            profile_param_arr.append(profile_param)
        else:
            feature_file_path = self._global_config.get_rd_hist_file_path(workload_type, workload_name)
            if not feature_file_path.exists():
                profile_param = {
                    "trace_path": self._global_config.get_block_cache_trace_path(workload_type, workload_name),
                    "feature_path": feature_file_path
                }
                profile_param_arr.append(profile_param)

        if batch_size < 1:
            batch_size = cpu_count()
        
        param_list = [[] for _ in range(batch_size)]
        for sample_index, sample_param in enumerate(profile_param_arr):
            param_list[sample_index % batch_size].append(sample_param)

        with Pool(batch_size) as process_pool:
            process_pool.map(self.profile_process, param_list)