"""This script generates the required samples based on our configurations.

It uses the multiprocessing module for parallel processing and the Sampler 
class from cydonia module to generate samples.  

Typical usage example:
    create_samples = CreateSamples()
    create_samples.create(workload_name)
"""

from json import dump
from itertools import product 
from multiprocessing import cpu_count, Pool

from cydonia.sample.Sample import sample 

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


class CreateSamples:
    def __init__(
            self,
            global_config: GlobalConfig = GlobalConfig(),
            sample_config: SampleExperimentConfig = SampleExperimentConfig()
    ) -> None:
        """This class generates samples in parallel using multiprocessing.
        
        Attributes:
            _global_config: Global configuration of experiments. 
            _sample_config: Configuration of sampling experiments. 
        """
        self._global_config = global_config
        self._sample_config = sample_config

    
    def sample_process(
            self,
            sample_param_arr: list 
    ) -> None:
        """Function called by a process that is running sampling. 

        Args:
            sample_param_arr: Array of parameters of sampling to run. 
        """
        for sample_params in sample_param_arr:
            sample_trace_path = sample_params["sample_trace_path"]
            block_trace_path = sample_params["block_trace_path"]
            rate, seed, bits = sample_params["rate"], sample_params["seed"], sample_params["bits"]
            sample_stat_path = sample_params["sample_stat_path"]
            if sample_stat_path.exists():
                continue 
            
            sample_trace_path.parent.mkdir(exist_ok=True, parents=True)
            sample_stat_path.parent.mkdir(exist_ok=True, parents=True)

            print("Generating samples {}".format(sample_trace_path))
            sample_stats = sample(block_trace_path, rate/100, seed, bits, sample_trace_path)
            with sample_stat_path.open("w+") as stat_file_handle:
                dump(sample_stats, stat_file_handle, indent=2)
            print("Generating sample {} and stat file {} completed!".format(sample_trace_path, sample_stat_path))
    

    def create(
            self, 
            workload_name: str,
            workload_type: str = "cp",
            batch_size = 4,
            sample_type: str = "iat"
    ) -> None:
        """Create sample traces from a given block trace.
        
        Args:
            workload_name: Name of the workload.
            workload_type: Type of workload. 
            batch_size: Number of sampling processers to start at once. 
            sample_type: The type of sampling technique used. 
        """
        sample_param_arr = []
        seed_arr, bits_arr, rate_arr = self._sample_config.seed_arr, self._sample_config.bits_arr, self._sample_config.rate_arr
        for seed, bits, rate in product(seed_arr, bits_arr, rate_arr):
            sample_feature_file_path = self._sample_config.get_split_feature_path(sample_type, workload_type, workload_name, rate, bits, seed, global_config=self._global_config)
            if sample_feature_file_path.exists():
                continue 

            block_trace_path = self._global_config.get_block_trace_path(workload_type, workload_name)
            sample_trace_path = self._sample_config.get_sample_trace_path(sample_type, workload_type, workload_name, rate, bits, seed, global_config=self._global_config)
            sampling_param_dict = {
                "block_trace_path": block_trace_path,
                "sample_trace_path": sample_trace_path,
                "rate": rate,
                "bits": bits, 
                "seed": seed,
                "sample_type": sample_type,
                "sample_stat_path": sample_feature_file_path
            }
            
            sample_param_arr.append(sampling_param_dict)
        
        if batch_size < 1:
            batch_size = cpu_count()

        param_list = [[] for _ in range(batch_size)]
        for sample_index, sample_param in enumerate(sample_param_arr):
            param_list[sample_index % batch_size].append(sample_param)
        
        with Pool(batch_size) as process_pool:
            process_pool.map(self.sample_process, param_list)