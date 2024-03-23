from json import load 

from cydonia.profiler.BAFM import BAFM
from cydonia.profiler.WorkloadStats import WorkloadStats

from keyuri.config.BaseConfig import BaseConfig
from keyuri.tracker.sample import get_workload_list_with_complete_sample_set_per_random_seed


def get_sample_blk_err(
        workload_name: str,
        rate: int,
        bits: int,
        seed: int,
        config: BaseConfig = BaseConfig(),
        sample_set_name: str = "basic"
) -> dict:
    full_cache_feature_file_path = config.get_cache_features_path(workload_name)
    if not full_cache_feature_file_path.exists():
        print("Full cache feature file {} does not exist.".format(full_cache_feature_file_path))
        return 
    
    with full_cache_feature_file_path.open("r") as handle:
        full_cache_feature_dict = load(handle)
        full_workload_stat = WorkloadStats()
        full_workload_stat.load_dict(full_cache_feature_dict)

    sample_cache_feature_file_path = config.get_sample_cache_features_path(sample_set_name, workload_name, rate, bits, seed)
    if not sample_cache_feature_file_path.exists():
        print("Sample cache feature file {} does not exist.".format(sample_cache_feature_file_path))
        return 
    
    with sample_cache_feature_file_path.open("r") as handle:
        sample_cache_feature_dict = load(handle)
        sample_workload_stat = WorkloadStats()
        sample_workload_stat.load_dict(sample_cache_feature_dict)
    
    error_dict = BAFM.get_error_dict(full_workload_stat.get_workload_feature_dict(), 
                                        sample_workload_stat.get_workload_feature_dict())
    error_dict["bits"] = bits
    error_dict["rate"] = rate
    return error_dict

    



class ProfileSampleCacheTrace:
    def __init__(
            self,
            sample_type: str,
            random_seed: int,
            config: BaseConfig = BaseConfig()
    ) -> None:
        self._type = sample_type
        self._seed = random_seed
        self._config = config

    
    def get_complete_workload_sets(self):
        return get_workload_list_with_complete_sample_set_per_random_seed(self._type, self._seed)
    

    def profile(
            self,
            workload_name: str
    ):
        full_cache_feature_file_path = self._config.get_cache_features_path(workload_name)
        if not full_cache_feature_file_path.exists():
            print("Full cache feature file {} does not exist.".format(full_cache_feature_file_path))
            return 
        
        with full_cache_feature_file_path.open("r") as handle:
            full_cache_feature_dict = load(handle)
            full_workload_stat = WorkloadStats()
            full_workload_stat.load_dict(full_cache_feature_dict)
        

        