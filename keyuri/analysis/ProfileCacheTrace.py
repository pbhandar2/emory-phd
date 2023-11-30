"""This module computes error in workload features by comparing
the full and sample traces.
"""

from pathlib import Path 
from pandas import read_csv 
from json import dump, JSONEncoder
from numpy import ndarray, int64

from keyuri.config.BaseConfig import BaseConfig
from cydonia.profiler.CacheTraceProfiler import get_workload_features_from_cache_trace


class NumpyEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ndarray):
            return obj.tolist()
        elif isinstance(obj, int64):
            return int(obj)
        return JSONEncoder.default(self, obj)


class ProfileCacheTrace:
    def __init__(
            self,
            workload_name: str,
            workload_type: str = "cp",
            sample_type: str = "iat"
    ) -> None:
        self._config = BaseConfig()
        self._workload_name = workload_name
        self._workload_type = workload_type 
        self._sample_type = sample_type 
    

    def profile(self, rate, bits, seed, force_flag=False):
        source_cache_trace_path = self._config.get_cache_trace_path(self._workload_name)
        source_cache_feature_path = self._config.get_cache_features_path(self._workload_name)

        if not source_cache_feature_path.exists() or force_flag:
            print("Computing cache features for {} at {}.".format(source_cache_trace_path, source_cache_feature_path))
            source_cache_feature_dict = self.compute_base_features(source_cache_trace_path)
            source_cache_feature_path.parent.mkdir(exist_ok=True, parents=True)
            with source_cache_feature_path.open("w+") as cache_feature_handle:
                dump(source_cache_feature_dict, cache_feature_handle, cls=NumpyEncoder)
        else:
            print("Already done cache features for {} at {}.".format(source_cache_trace_path, source_cache_feature_path))
        
        sample_cache_trace_path_list = self._config.get_all_sample_cache_traces(self._sample_type, self._workload_name)
        for sample_cache_trace_path in sample_cache_trace_path_list:
            if '.rd' in sample_cache_trace_path.name:
                continue 
            split_sample_file_name = sample_cache_trace_path.stem.split('_')
            cur_rate, cur_bits, cur_seed = int(split_sample_file_name[0]), int(split_sample_file_name[1]), int(split_sample_file_name[2])

            if rate != cur_rate or bits != cur_bits or seed != cur_seed:
                continue 

            sample_cache_feature_path = self._config.get_sample_cache_features_path(self._sample_type,
                                                                                        self._workload_name,
                                                                                        rate,
                                                                                        bits,
                                                                                        seed)

            if not sample_cache_feature_path.exists() or force_flag:
                print("Computing cache features for {} at {}.".format(sample_cache_trace_path, sample_cache_feature_path))
                sample_cache_feature_dict = self.compute_base_features(sample_cache_trace_path)
                sample_cache_feature_path.parent.mkdir(exist_ok=True, parents=True)
                with sample_cache_feature_path.open("w+") as sample_cache_feature_handle:
                    dump(sample_cache_feature_dict, sample_cache_feature_handle, cls=NumpyEncoder)
            else:
                print("Already done cache features for {} at {}.".format(sample_cache_trace_path, sample_cache_feature_path))


    @staticmethod
    def compute_base_features(cache_trace_path: Path) -> dict:
        df = read_csv(cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])
        return get_workload_features_from_cache_trace(df)