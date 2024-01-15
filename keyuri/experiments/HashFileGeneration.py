from keyuri.config.BaseConfig import BaseConfig
from cydonia.profiler.CacheTrace import CacheTraceReader


class HashFileGeneration:
    def __init__(self, workload_name, random_seed: int, num_lower_addr_bits_ignored: int):
        self._workload = workload_name
        self._config = BaseConfig()
        self._num_lower_addr_bits_ignored = num_lower_addr_bits_ignored
        self._random_seed = random_seed


    def generate_hash_file(self) -> None:
        hash_file_path = self._config.get_sample_hash_file_path(self._workload, self._random_seed, self._num_lower_addr_bits_ignored)
        cache_trace_path = self._config.get_cache_trace_path(self._workload)

        if not hash_file_path.exists():
            print("Generating hash file {}".format(hash_file_path))
            cache_trace = CacheTraceReader(cache_trace_path)
            hash_file_path.parent.mkdir(exist_ok=True, parents=True)
            cache_trace.create_sample_hash_file(self._random_seed, self._num_lower_addr_bits_ignored, hash_file_path)
        else:
            print("Hash file {} already exists!".format(hash_file_path))