"""This script generates features from the sample and compares it to the features of the original. 

It uses the multiprocessing module for parallel processing, and the BlockTraceProfiler and BlockAccessTraceProfiler 
from cydonia package for profiling the samples. 

Typical usage example:
    profile_samples = ProfileSamples()
    profile_samples.profile()
"""

from json import dump
from itertools import product 
from pathlib import Path 
from multiprocessing import cpu_count, Pool

from cydonia.profiler.CPReader import CPReader
from cydonia.profiler.BlockTraceProfiler import BlockTraceProfiler

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


class ProfileSamples:
    def __init__(self) -> None:
        """This class profiles samples in parallel using multiprocessing.
        
        Attributes:
            _global_config: Global configuration of experiments. 
            _sample_config: Configuration of sampling experiments. 
        """
        self._global_config = GlobalConfig()
        self._sample_config = SampleExperimentConfig()


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
            reader = CPReader(profile_params["trace_path"])
            profiler = BlockTraceProfiler(reader)
            profiler.run()

            stat = profiler.get_stat()
            profile_params["feature_path"].parent.mkdir(exist_ok=True, parents=True)
            with profile_params["feature_path"].open("w+") as feature_file_handle:
                dump(stat, feature_file_handle, indent=2)


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
            trace_path = self._sample_config.get_sample_trace_path(sample_type, workload_type, workload_name, rate, bits, seed)
            if not trace_path.exists():
                continue 

            feature_file_path = self._sample_config.get_block_feature_file_path(sample_type, workload_type, workload_name, rate, bits, seed)
            if feature_file_path.exists():
                if feature_file_path.is_dir():
                    feature_file_path.rmdir()
                else:
                    continue 

            profile_param = {
                "trace_path": trace_path,
                "feature_path": feature_file_path
            }
            profile_param_arr.append(profile_param)
        else:
            feature_file_path = self._global_config.get_block_feature_file_path(workload_type, workload_name)
            if not feature_file_path.exists():
                profile_param = {
                    "trace_path": self._global_config.get_block_trace_path(workload_type, workload_name),
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