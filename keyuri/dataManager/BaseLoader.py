from pathlib import Path 

from keyuri.config.BaseConfig import BaseConfig


def get_all_cp_workloads():
    return ["w{}".format(i) if i > 9 else "w0{}".format(i) for i in range(1, 107)]


def get_samples_not_profiled(
        sample_trace_dir: Path, 
        sample_feature_dir: Path
) -> list:
    samples_missing_feature_file = []
    for sample_cache_trace_file in sample_trace_dir.iterdir():
        sample_file_name = sample_cache_trace_file.name 
        sample_cache_feature_file = sample_feature_dir.joinpath(sample_file_name)
        if not sample_cache_feature_file.exists():
            samples_missing_feature_file.append(sample_cache_trace_file)
    return samples_missing_feature_file


def get_workload_feature(cache_feature_dict: dict) -> dict:
    pass 


class BaseLoader:
    def __init__(self, config: BaseConfig = BaseConfig()):
        self._config = config 
        self._sample_set_name = "basic"
        self._workload_list = ["w{}".format(i) if i>9 else "w0{}".format(i) for i in range(1, 107)]
    

    def get_all_sample_feature_files(self):
        sample_feature_file_list = []
        for workload_name in self._workload_list:
            full_cache_feature_file_path = self._config.get_cache_features_path(workload_name)
            for sample_file_path in self._config.get_all_sample_cache_features(self._sample_set_name, workload_name):
                print(sample_file_path)
                print(full_cache_feature_file_path)


                

