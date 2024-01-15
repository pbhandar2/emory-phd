from itertools import product 

from keyuri.config.BaseConfig import BaseConfig

from cydonia.profiler.BAFM import BAFM


class AccessMap:
    def __init__(
            self, 
            workload: str, 
            sample_set_name: str,
            config: BaseConfig = BaseConfig()
    ) -> None:
        self._sample_set_name = sample_set_name 
        self._workload = workload
        self._config = config 
    

    def create(
            self, 
            rate_arr: list, 
            bits_arr: list, 
            seed_arr: list
    ) -> None:
        assert all([rate > 0.0 and rate < 1.0 for rate in rate_arr])
        for bits, seed, rate in product(bits_arr, seed_arr, rate_arr):
            sample_file_path = self._config.get_sample_cache_trace_path(self._sample_set_name,
                                                                            self._workload,
                                                                            int(100*rate),
                                                                            bits,
                                                                            seed)
            
            if not sample_file_path.exists():
                print("Sample file {} does not exist.".format(sample_file_path))
                continue 

            access_file_path = self._config.get_sample_access_feature_file_path(self._sample_set_name,
                                                                                    self._workload,
                                                                                    int(100*rate),
                                                                                    bits,
                                                                                    seed)
            
            if access_file_path.exists():
                print("Access file already exists! {}".format(access_file_path))
                continue 

            print("Creating access file {}.".format(access_file_path))
            bafm = BAFM(bits)
            access_file_path.parent.mkdir(parents=True, exist_ok=True)
            bafm.load_cache_trace(sample_file_path)
            bafm.write_map_to_file(access_file_path)