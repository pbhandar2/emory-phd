from pathlib import Path 
from unittest import main, TestCase

from keyuri.dataManager.BaseLoader import BaseLoader, get_samples_not_profiled, get_all_cp_workloads


WORKLOAD_TYPE = "cp"
SAMPLE_TYPE = "basic"
FEATURE_DIR_NAME = "cache_features"
SAMPLE_DIR_NAME = "cache_traces"
DATA_DIR = Path("/research2/mtc/cp_traces/pranav-phd")

SAMPLE_FEATURE_DIR = DATA_DIR.joinpath("{}--{}".format(WORKLOAD_TYPE, SAMPLE_TYPE), FEATURE_DIR_NAME)
SAMPLE_TRACE_DIR = DATA_DIR.joinpath("{}--{}".format(WORKLOAD_TYPE, SAMPLE_TYPE), SAMPLE_DIR_NAME)
FULL_FEATURE_DIR = DATA_DIR.joinpath(WORKLOAD_TYPE, FEATURE_DIR_NAME)


class TestDataManager(TestCase):
    def test_basic(self):
        base_loader = BaseLoader()
        base_loader.get_all_sample_feature_files()
    

    def test_get_sample_not_profiled(self):
        for workload_name in get_all_cp_workloads():
            sample_trace_dir = SAMPLE_TRACE_DIR.joinpath(workload_name)
            sample_feature_dir = SAMPLE_FEATURE_DIR.joinpath(workload_name)
            sample_trace_files_not_profiled = get_samples_not_profiled(sample_trace_dir, sample_feature_dir)


if __name__ == '__main__':
    main()