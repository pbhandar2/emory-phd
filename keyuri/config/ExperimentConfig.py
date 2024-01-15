from numpy import array, ndarray

from keyuri.config.BaseConfig import BaseConfig


DEFAULT_LOWER_BITS_IGNORE_ARR = array([4, 2, 1, 0], dtype=int)
DEFAULT_RANDOM_SEED_ARR = array([42, 43, 44], dtype=int)
DEFAULT_SAMPLE_RATE_ARR = array([0.01, 0.05, 0.1, 0.2, 0.4, 0.8], dtype=float)


class ExperimentConfig:
    def __init__(
            self,
            lower_addr_bits_ignored_arr: ndarray = DEFAULT_LOWER_BITS_IGNORE_ARR,
            random_seed_arr: ndarray = DEFAULT_RANDOM_SEED_ARR,
            sample_rate_arr: ndarray = DEFAULT_SAMPLE_RATE_ARR,
            base_config: BaseConfig = BaseConfig()
    ) -> None:
        self.lower_addr_bits_ignored_arr = lower_addr_bits_ignored_arr
        self.random_seed_arr = random_seed_arr
        self.sample_rate_arr = sample_rate_arr
        self.base_config = base_config
    

    def get_sample_cache_feature_set(
            self,
            sample_set_name: str,
            workload_name: str, 
            lower_addr_bits_ignored: int,
            random_seed: int,
            sample_rate_arr: ndarray = DEFAULT_SAMPLE_RATE_ARR
    ) -> tuple:
        full_feature_set_exists = True 
        sample_cache_feature_file_list = []
        for sample_rate in sample_rate_arr:
            sample_cache_feature_file_path = self.base_config.get_sample_cache_features_path(sample_set_name,
                                                                                                workload_name,
                                                                                                int(sample_rate*100),
                                                                                                lower_addr_bits_ignored,
                                                                                                random_seed)
            if not sample_cache_feature_file_path.exists():
                full_feature_set_exists = False 
            sample_cache_feature_file_list.append(sample_cache_feature_file_path)
        
        return full_feature_set_exists, sample_cache_feature_file_list