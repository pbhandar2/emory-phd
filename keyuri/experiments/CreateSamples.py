"""This script generates the required samples based on our configurations.

It uses the multiprocessing module for parallel processing and the Sampler 
class from cydonia module to generate samples.  

Typical usage example:
    create_samples = CreateSamples()
    create_samples.create(workload_name)
"""

from json import dump
from pathlib import Path 
from itertools import product 
from multiprocessing import cpu_count, Pool
from collections import Counter 
from numpy import zeros, ceil, array, mean, percentile
from pandas import read_csv 

from cydonia.sample.Sampler import Sampler

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


class CreateSamples:
    def __init__(self) -> None:
        """This class generates samples in parallel using multiprocessing.
        
        Attributes:
            _global_config: Global configuration of experiments. 
            _sample_config: Configuration of sampling experiments. 
        """
        self._global_config = GlobalConfig()
        self._sample_config = SampleExperimentConfig()

    
    def sample_process(
            self,
            sample_param_arr: list 
    ) -> None:
        """Function called by a process that is running sampling. 
        
        It runs a set of sampling allocated to this process. 

        Args:
            sample_param_arr: Array of parameters of sampling to run. 
        """
        for sample_params in sample_param_arr:
            sample_trace_path = sample_params["sample_trace_path"]
            block_trace_path = sample_params["block_trace_path"]
            rate, seed, bits = sample_params["rate"], sample_params["seed"], sample_params["bits"]
            sample_type = sample_params["sample_type"]
            sample_stat_path = sample_params["sample_stat_path"]
            if sample_stat_path.exists():
                continue 
            
            sample_trace_path.parent.mkdir(exist_ok=True, parents=True)
            sample_stat_path.parent.mkdir(exist_ok=True, parents=True)

            print("Generating samples {}".format(sample_trace_path))
            sampler = Sampler(str(block_trace_path.absolute()))
            split_counter = sampler.sample(rate, seed, bits, sample_type, sample_trace_path)
            split_stats = self.get_stats_from_split_counter(split_counter, rate, seed, bits, sample_trace_path)

            with sample_stat_path.open("w+") as stat_file_handle:
                dump(split_stats, stat_file_handle, indent=2)
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
            sample_feature_file_path = self._sample_config.get_split_feature_path(sample_type, workload_type, workload_name, rate, bits, seed)
            if sample_feature_file_path.exists():
                continue 

            block_trace_path = self._global_config.get_block_trace_path(workload_type, workload_name)
            sample_trace_path = self._sample_config.get_sample_trace_path(sample_type, workload_type, workload_name, rate, bits, seed)
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
        

    def get_stats_from_split_counter(
            self, 
            split_counter: Counter, 
            sample_rate: int, 
            seed: int, 
            bits: int, 
            sample_file_path: Path,
            percentile_arr: list = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 99.9, 100]
    ) -> dict:
        """Get statistics (mean, min, max, percentiles) from the counter of number of samples 
        generated from a sampled block request. 

        Args:
            split_counter: Counter of the number of samples generated per sampled block request 

        Returns:
            stats: Dictionary of statistics of the sample. 
        """
        total_request_sampled = sum(split_counter.values())
        stats = {}
        if total_request_sampled > 0:
            split_array = self.generate_array_from_counter(split_counter)

            stats['mean'] = mean(split_array) 
            stats['total'] = len(split_array)

            for _, percentile_val in enumerate(percentile_arr):
                stats['p_{}'.format(percentile_val)] = percentile(split_array, percentile_val, keepdims=False)
            
            no_split_count = len(split_array[split_array == 1])
            stats['freq%'] = int(ceil(100*(stats['total'] - no_split_count)/stats['total']))

            sample_df = read_csv(sample_file_path, names=["ts", "lba", "op", "size"])
            stats['rate'] = sample_rate 
            stats['seed'] = seed 
            stats['bits'] = bits 
            stats['unique_lba_count'] = int(sample_df['lba'].nunique())
        else:
            stats = {
                'mean': 0,
                'total': 0,
                'freq%': 0,
                'rate': sample_rate,
                'seed': seed,
                'bits': bits,
                'unique_lba_count': 0
            }
            for _, percentile_val in enumerate(percentile_arr):
                stats['p_{}'.format(percentile_val)] = 0
        return stats 
    

    def generate_array_from_counter(
            self, 
            counter: Counter
    ) -> array:
        """Generate array representing the frequency of items in the counter. 

        Args:
            counter: Counter with item as key and frequency as value. 

        Returns:
            array: Array with items with frequency matching in the counter. 
        """
        total_items = sum(counter.values())
        array = zeros(total_items, dtype=int)

        cur_index = 0 
        for key in counter:
            array[cur_index:cur_index+counter[key]] = key
            cur_index += counter[key]
        return array 