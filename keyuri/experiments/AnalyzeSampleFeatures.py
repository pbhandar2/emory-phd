from pathlib import Path 
from json import load, dump 
from itertools import product 
from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


class AnalyzeSampleFeatures:
    def __init__(self) -> None:
        """This class analyzes and compares the features of samples and full block trace. 
        
        Attributes:
            _global_config: Global configuration of experiments. 
            _sample_config: Configuration of sampling experiments. 
        """
        self._global_config = GlobalConfig()
        self._sample_config = SampleExperimentConfig()
    

    def analyze(
            self,
            workload_name: str, 
            workload_type: str = "cp",
            sample_type: str = "iat"
    ) -> None:
        """Analyze the difference in the features between sample and original. 

        Args:
            workload_name: Name of the workload.
            workload_type: Type of workload. 
            sample_type: The type of sampling technique used. 
        """
        full_feature_file_path = self._global_config.get_block_feature_file_path(workload_type, workload_name)
        with full_feature_file_path.open("r") as full_feature_handle:
            full_trace_feature_dict = load(full_feature_handle)
        
        seed_arr, bits_arr, rate_arr = self._sample_config.seed_arr, self._sample_config.bits_arr, self._sample_config.rate_arr
        for seed, bits, rate in product(seed_arr, bits_arr, rate_arr):
            sample_trace_feature_file_path = self._sample_config.get_block_feature_file_path(sample_type, workload_type, workload_name, rate, bits, seed)
            if not sample_trace_feature_file_path.exists():
                continue 

            percent_diff_feature_file_path = self._sample_config.get_percent_diff_feature_file_path(sample_type, workload_type, workload_name, rate, bits, seed)
            if percent_diff_feature_file_path.exists():
                continue 

            with sample_trace_feature_file_path.open('r') as sample_feature_handle:
                sample_trace_feature_dict = load(sample_feature_handle)
            
            percent_diff_stats = {
                "rate": rate,
                "bits": bits, 
                "rate": rate,
                "name": workload_name,
                "type": workload_type
            }
            for feature_name in sample_trace_feature_dict:
                if feature_name not in full_trace_feature_dict:
                    continue 

                try:
                    sample_feature_val = float(sample_trace_feature_dict[feature_name])
                    full_feature_val = float(full_trace_feature_dict[feature_name])
                    percent_diff = 100*(full_feature_val - sample_feature_val)/full_feature_val
                    percent_diff_stats[feature_name] = percent_diff
                except:
                    pass 
            
            print("Computed percent diff for files {}, {}".format(sample_trace_feature_file_path, percent_diff_feature_file_path))
            percent_diff_feature_file_path.parent.mkdir(exist_ok=True, parents=True)
            with percent_diff_feature_file_path.open('w+') as percent_diff_file_handle:
                dump(percent_diff_stats, percent_diff_file_handle, indent=2)