from keyuri.config.BaseConfig import BaseConfig
from cydonia.profiler.CacheTrace import CacheTraceReader


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
        cache_trace = CacheTraceReader(cache_trace_path)
        block_stat = cache_trace.get_stat()
        cache_feature_path.parent.mkdir(exist_ok=True, parents=True)
        block_stat.write_to_file(cache_feature_path)