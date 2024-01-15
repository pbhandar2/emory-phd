from keyuri.config.BaseConfig import BaseConfig
from keyuri.tracker.sample import get_workload_list_with_complete_sample_set_per_random_seed


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
    

    def profile(self):
        workload_list = self.get_complete_workload_sets()
        for workload in workload_list:
            pass 
        