from numpy import ndarray, array 
from itertools import product 
from pathlib import Path 

from keyuri.config.ExperimentConfig import DEFAULT_LOWER_BITS_IGNORE_ARR, DEFAULT_RANDOM_SEED_ARR, DEFAULT_SAMPLE_RATE_ARR
from keyuri.config.BaseConfig import BaseConfig


def get_cp_workload_name_list():
    return ["w{}".format(i) if i>9 else "w0{}".format(i) for i in range(1, 107)]


def get_workload_list_with_complete_sample_set_per_random_seed(
        sample_set_name: str,
        random_seed: int
) -> list:
    """ Get list of workloads with default samples generated.
    
    Args:
        sample_set_name: The name of sample set.
        random_seed: Random seed during sampling.
    
    Return:
        workload_name_list: List of workload names which was default samples generated.
    """
    workload_name_list = []
    for workload_name in get_cp_workload_name_list():
        if check_if_samples_generated(sample_set_name, workload_name, random_seed_arr=array([random_seed], dtype=int)):
            workload_name_list.append(workload_name)
    return workload_name_list


def check_if_samples_generated(
        sample_set_name: str,
        workload_name: str,
        random_seed_arr: ndarray = DEFAULT_RANDOM_SEED_ARR,
        sampling_rate_arr: ndarray = DEFAULT_SAMPLE_RATE_ARR,
        lower_bits_ignore_arr: ndarray = DEFAULT_LOWER_BITS_IGNORE_ARR,
        base_config: BaseConfig = BaseConfig()
) -> bool:
    """ Check if all samples related to the sample set is generated.

    Args:
        sample_set_name: The name of sample set.
        workload_name: The name of the workload.
        random_seed: Random seed during sampling.
        sampling_rate_arr: Array of sampling rates used in this sample set.
        lower_bits_ignore_arr: Array of lower bits ignored in this sample set.
        base_config: BaseConfig of experiment files.
    """
    for random_seed, sampling_rate, lower_bits_ignore in product(random_seed_arr, sampling_rate_arr, lower_bits_ignore_arr):
        assert sampling_rate > 0.0 and sampling_rate < 1.0,\
            "Sampling rate should be > 0 and < 1 but found {}.".format(sampling_rate)

        sample_file_path = base_config.get_sample_cache_trace_path(sample_set_name, 
                                                                        workload_name, 
                                                                        int(100*sampling_rate), 
                                                                        lower_bits_ignore, 
                                                                        random_seed)
        if not sample_file_path.exists():
            print("sample file path {} does not exist.".format(sample_file_path))
            return False 
        
    return True


def get_sample_hit_rate_error_file_list(
        sample_set_name: str,
        workload_name: str,
        random_seed_arr: ndarray = DEFAULT_RANDOM_SEED_ARR,
        sampling_rate_arr: ndarray = DEFAULT_SAMPLE_RATE_ARR,
        lower_bits_ignore_arr: ndarray = DEFAULT_LOWER_BITS_IGNORE_ARR,
        base_config: BaseConfig = BaseConfig()
) -> list:
    sample_error_file_list = []
    for random_seed, sampling_rate, lower_bits_ignore in product(random_seed_arr, sampling_rate_arr, lower_bits_ignore_arr):
        assert sampling_rate > 0.0 and sampling_rate < 1.0,\
            "Sampling rate should be > 0 and < 1 but found {}.".format(sampling_rate)

        sample_file_path = base_config.get_hit_rate_error_file_path(sample_set_name, 
                                                                            workload_name, 
                                                                            int(100*sampling_rate), 
                                                                            lower_bits_ignore, 
                                                                            random_seed)
        
        sample_error_file_list.append(sample_file_path)
    return sample_error_file_list


def get_sample_error_file_list(
        sample_set_name: str,
        workload_name: str,
        random_seed_arr: ndarray = DEFAULT_RANDOM_SEED_ARR,
        sampling_rate_arr: ndarray = DEFAULT_SAMPLE_RATE_ARR,
        lower_bits_ignore_arr: ndarray = DEFAULT_LOWER_BITS_IGNORE_ARR,
        base_config: BaseConfig = BaseConfig()
) -> list:
    sample_error_file_list = []
    for random_seed, sampling_rate, lower_bits_ignore in product(random_seed_arr, sampling_rate_arr, lower_bits_ignore_arr):
        assert sampling_rate > 0.0 and sampling_rate < 1.0,\
            "Sampling rate should be > 0 and < 1 but found {}.".format(sampling_rate)

        sample_file_path = base_config.get_sample_block_error_file_path(sample_set_name, 
                                                                            workload_name, 
                                                                            int(100*sampling_rate), 
                                                                            lower_bits_ignore, 
                                                                            random_seed)
        
        sample_error_file_list.append(sample_file_path)
    return sample_error_file_list


def get_sample_cache_feature_file_list(
        sample_set_name: str,
        workload_name: str,
        random_seed_arr: ndarray = DEFAULT_RANDOM_SEED_ARR,
        sampling_rate_arr: ndarray = DEFAULT_SAMPLE_RATE_ARR,
        lower_bits_ignore_arr: ndarray = DEFAULT_LOWER_BITS_IGNORE_ARR,
        base_config: BaseConfig = BaseConfig()
) -> list:
    sample_feature_file_list = []
    for random_seed, sampling_rate, lower_bits_ignore in product(random_seed_arr, sampling_rate_arr, lower_bits_ignore_arr):
        assert sampling_rate > 0.0 and sampling_rate < 1.0,\
            "Sampling rate should be > 0 and < 1 but found {}.".format(sampling_rate)

        sample_file_path = base_config.get_sample_cache_features_path(sample_set_name, 
                                                                        workload_name, 
                                                                        int(100*sampling_rate), 
                                                                        lower_bits_ignore, 
                                                                        random_seed)
        
        sample_feature_file_list.append(sample_file_path)
    return sample_feature_file_list
        

def check_if_sample_features_computed(
        sample_set_name: str,
        workload_name: str,
        random_seed_arr: ndarray = DEFAULT_RANDOM_SEED_ARR,
        sampling_rate_arr: ndarray = DEFAULT_SAMPLE_RATE_ARR,
        lower_bits_ignore_arr: ndarray = DEFAULT_LOWER_BITS_IGNORE_ARR,
        base_config: BaseConfig = BaseConfig()
) -> bool:
    """ Check if all samples related to the sample set is generated.

    Args:
        sample_set_name: The name of sample set.
        workload_name: The name of the workload.
        random_seed: Random seed during sampling.
        sampling_rate_arr: Array of sampling rates used in this sample set.
        lower_bits_ignore_arr: Array of lower bits ignored in this sample set.
        base_config: BaseConfig of experiment files.
    """
    cache_feature_file_list = get_sample_cache_feature_file_list(sample_set_name,
                                                                    workload_name,
                                                                    random_seed_arr=random_seed_arr,
                                                                    sampling_rate_arr=sampling_rate_arr,
                                                                    lower_bits_ignore_arr=lower_bits_ignore_arr,
                                                                    base_config=base_config)
    
    for cache_feature_file_path in cache_feature_file_list:
        if not cache_feature_file_path.exists():
            return False 
    
    return True 


def check_if_sample_error_set(
        sample_set_name: str,
        workload_name: str,
        random_seed_arr: ndarray = DEFAULT_RANDOM_SEED_ARR,
        sampling_rate_arr: ndarray = DEFAULT_SAMPLE_RATE_ARR,
        lower_bits_ignore_arr: ndarray = DEFAULT_LOWER_BITS_IGNORE_ARR,
        base_config: BaseConfig = BaseConfig()
) -> bool:
    """ Check if all samples related to the sample set is generated.

    Args:
        sample_set_name: The name of sample set.
        workload_name: The name of the workload.
        random_seed: Random seed during sampling.
        sampling_rate_arr: Array of sampling rates used in this sample set.
        lower_bits_ignore_arr: Array of lower bits ignored in this sample set.
        base_config: BaseConfig of experiment files.
    """
    cache_feature_file_list = get_sample_error_file_list(sample_set_name,
                                                            workload_name,
                                                            random_seed_arr=random_seed_arr,
                                                            sampling_rate_arr=sampling_rate_arr,
                                                            lower_bits_ignore_arr=lower_bits_ignore_arr,
                                                            base_config=base_config)
    
    for cache_feature_file_path in cache_feature_file_list:
        if not cache_feature_file_path.exists():
            print("Cache feature file {} does not exist.".format(cache_feature_file_path))
            return False 
    
    return True 


def check_if_hit_rate_error_set(
        sample_set_name: str,
        workload_name: str,
        random_seed_arr: ndarray = DEFAULT_RANDOM_SEED_ARR,
        sampling_rate_arr: ndarray = DEFAULT_SAMPLE_RATE_ARR,
        lower_bits_ignore_arr: ndarray = DEFAULT_LOWER_BITS_IGNORE_ARR,
        base_config: BaseConfig = BaseConfig()
) -> bool:
    """ Check if all samples related to the sample set is generated.

    Args:
        sample_set_name: The name of sample set.
        workload_name: The name of the workload.
        random_seed: Random seed during sampling.
        sampling_rate_arr: Array of sampling rates used in this sample set.
        lower_bits_ignore_arr: Array of lower bits ignored in this sample set.
        base_config: BaseConfig of experiment files.
    """
    cache_feature_file_list = get_sample_hit_rate_error_file_list(sample_set_name,
                                                                    workload_name,
                                                                    random_seed_arr=random_seed_arr,
                                                                    sampling_rate_arr=sampling_rate_arr,
                                                                    lower_bits_ignore_arr=lower_bits_ignore_arr,
                                                                    base_config=base_config)
    
    for cache_feature_file_path in cache_feature_file_list:
        if not cache_feature_file_path.exists():
            print("Cache feature file {} does not exist.".format(cache_feature_file_path))
            return False 
    
    return True 