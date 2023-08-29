from itertools import product 
from multiprocessing import cpu_count, Pool

from cydonia.profiler.CacheTrace import CacheTrace

from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


class CreateCacheTrace:
    def __init__(self, stack_binary_path) -> None:
        """This class generates cache traces from block storage traces. 
        
        Attributes:
            _global_config: Global configuration of experiments. 
            _sample_config: Configuration of sampling experiments. 
        """
        self._global_config = GlobalConfig()
        self._sample_config = SampleExperimentConfig()
        self._stack_binary_path = stack_binary_path

    
    def create_process(
            self,
            create_param_arr: list
    ) -> None:
        """Function called by a process that is creating cache traces. 

        Args:
            create_param_arr: Array of parameters of to create cache trace.  
        """
        for create_params in create_param_arr:
            block_trace_path, cache_trace_path = create_params["block"], create_params["cache"]
            cache_trace = CacheTrace(self._stack_binary_path)
            cache_trace_path.parent.mkdir(exist_ok=True, parents=True)
            cache_trace.generate_cache_trace(block_trace_path, cache_trace_path)
            print("Generated cache trace {} for block trace {}".format(cache_trace_path, block_trace_path))


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
        create_arr = []
        seed_arr, bits_arr, rate_arr = self._sample_config.seed_arr, self._sample_config.bits_arr, self._sample_config.rate_arr
        for seed, bits, rate in product(seed_arr, bits_arr, rate_arr):
            cache_trace_path = self._sample_config.get_sample_cache_trace_path(sample_type, workload_type, workload_name, rate, bits, seed)
            if cache_trace_path.exists():
                continue 

            block_trace_path = self._sample_config.get_sample_trace_path(sample_type, workload_type, workload_name, rate, bits, seed)
            if not block_trace_path.exists():
                continue 
            create_arr.append({
                "block": block_trace_path,
                "cache": cache_trace_path
            })
        else:
            cache_trace_path = self._global_config.get_block_cache_trace_path(workload_type, workload_name)
            if not cache_trace_path.exists():
                block_trace_path = self._global_config.get_block_trace_path(workload_type, workload_name)
                if block_trace_path.exists():
                    create_arr.append({
                        "block": block_trace_path,
                        "cache": cache_trace_path
                    })

        if batch_size < 1:
            batch_size = cpu_count()

        param_list = [[] for _ in range(batch_size)]
        for sample_index, sample_param in enumerate(create_arr):
            param_list[sample_index % batch_size].append(sample_param)
        
        with Pool(batch_size) as process_pool:
            process_pool.map(self.create_process, param_list)