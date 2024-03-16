from pathlib import Path 

from keyuri.config.BaseConfig import BaseConfig
from cydonia.profiler.CacheTrace import CacheTraceReader


def generate_next_sample_workload_feature_file(
        dir_config: BaseConfig = BaseConfig(),
        sample_set_name: str = "basic",
        check_for_size: bool = False 
) -> None:
    """ Generate a workload feature file if it does not exist for some sample. """
    for cache_trace_path in dir_config.get_all_cache_traces():
        workload_name = cache_trace_path.stem 
        sample_path_list = dir_config.get_all_sample_cache_traces(sample_set_name, workload_name)
        for sample_trace_path in sample_path_list:
            print(sample_trace_path)
            rate, bits, seed = dir_config.get_sample_file_info(sample_trace_path)
            sample_feature_path = dir_config.get_sample_cache_features_path(sample_set_name, workload_name, rate, bits, seed)
            if not sample_feature_path.exists():
                print("Generating sample feature file {} form sample trace at {}".format(sample_feature_path, sample_trace_path))
                generate_workload_feature_file(sample_trace_path, sample_feature_path)
            else:
                if check_for_size:
                    # TODO: implement check file size and if its 0 then 
                    pass 


def generate_workload_feature_file(
        cache_trace_path: Path, 
        cache_feature_path: Path
) -> None:
    """ Create workload feature file from a cache trace. """
    if cache_feature_path.exists():
        print("File already exists!")
        return True 

    cache_feature_path.parent.mkdir(exist_ok=True, parents=True)
    cache_feature_path.touch()

    cache_trace = CacheTraceReader(cache_trace_path)
    block_stat = cache_trace.get_stat()
    block_stat.write_to_file(cache_feature_path)



class CacheFeatures:
    def __init__(self, workload_name):
        self._workload = workload_name
        self._config = BaseConfig()


    def generate_cache_feature_files(self) -> None:
        cache_feature_path = self._config.get_cache_features_path(self._workload)
        cache_trace_path = self._config.get_cache_trace_path(self._workload)

        if not cache_feature_path.exists():
            print("Generating cache feature file {}".format(cache_feature_path))
            cache_trace = CacheTraceReader(cache_trace_path)
            block_stat = cache_trace.get_stat()
            cache_feature_path.parent.mkdir(exist_ok=True, parents=True)
            block_stat.write_to_file(cache_feature_path)
        else:
            print("Cache feature file {} already exists!".format(cache_feature_path))

    
    def generate_sample_cache_feature_files(
            self,
            sample_set_name: str, 
            num_lower_addr_bits_ignored: int,
            random_seed: int, 
            rate: float 
    ) -> None:
        cache_feature_path = self._config.get_sample_cache_features_path(sample_set_name, 
                                                                            self._workload,
                                                                            int(100*rate),
                                                                            num_lower_addr_bits_ignored,
                                                                            random_seed)

        cache_trace_path = self._config.get_sample_cache_trace_path(sample_set_name, 
                                                                        self._workload,
                                                                        int(100*rate),
                                                                        num_lower_addr_bits_ignored,
                                                                        random_seed)
        
        if not cache_trace_path.exists():
            print("Sample does not exist {}.".format(cache_trace_path))
            return 
        
        if cache_feature_path.exists():
            print("Sample cache features file {} already generated!".format(cache_feature_path))
            return 

        print("Generating cache features {} from sample {}.".format(cache_feature_path, cache_trace_path))
        generate_workload_feature_file(cache_trace_path, cache_feature_path)