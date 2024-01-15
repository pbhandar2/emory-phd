from itertools import product 

from keyuri.config.BaseConfig import BaseConfig

from cydonia.profiler.CacheTrace import CacheTraceReader, HashFile


class SampleCacheTrace:
    def __init__(
            self, 
            workload: str, 
            sample_set_name: str,
            config: BaseConfig = BaseConfig()
    ) -> None:
        self._sample_set_name = sample_set_name 
        self._workload = workload
        self._config = config 
    

    def sample(
            self, 
            rate_arr: list, 
            bits_arr: list, 
            seed_arr: list
    ) -> None:
        assert all([rate > 0.0 and rate < 1.0 for rate in rate_arr])
        cache_trace_path = self._config.get_cache_trace_path(self._workload)
        cache_reader = CacheTraceReader(cache_trace_path)
        for bits, seed in product(bits_arr, seed_arr):
            hash_file_path = self._config.get_sample_hash_file_path(self._workload, seed, bits)
            if not hash_file_path.exists():
                print("Hash file {} does not exist!".format(hash_file_path))
                continue 
            hash_file = HashFile(hash_file_path)
            hash_file.load()
            print("Loaded hash file {}".format(hash_file_path))
            for rate in rate_arr:
                sample_file_path = self._config.get_sample_cache_trace_path(self._sample_set_name,
                                                                                self._workload,
                                                                                int(100*rate),
                                                                                bits,
                                                                                seed)
                
                if sample_file_path.exists():
                    print("Sample file path {} already exists.".format(sample_file_path))
                    return 

                print("Sampling using hash file {}, rate {}, bits {} to create file {}.".format(hash_file_path,
                                                                                                    rate,
                                                                                                    bits,
                                                                                                    sample_file_path))
                sample_file_path.parent.mkdir(exist_ok=True, parents=True)
                cache_reader.sample_using_hash_file(hash_file_path, rate, bits, sample_file_path)