from pathlib import Path 
from numpy import arange 
from json import load, dumps, dump 
from itertools import product 
from keyuri.config.Config import GlobalConfig, SampleExperimentConfig

from cydonia.profiler.RDHistogram import RDHistogram


class AnalyzeSampleFeatures:
    def __init__(
            self,
            global_config: GlobalConfig = GlobalConfig(),
            sample_config: SampleExperimentConfig = SampleExperimentConfig()
    ) -> None:
        """This class analyzes and compares the features of samples and full block trace.
        
        Attributes:
            _global_config: Global configuration of experiments. 
            _sample_config: Configuration of sampling experiments. 
        """
        self._global_config = global_config
        self._sample_config = sample_config


    def analyze(
            self,
            workload_name: str, 
            workload_type: str = "cp",
            sample_type: str = "iat",
            force: bool = False 
    ) -> None:
        """Analyze the difference in the features between sample and original. 

        Args:
            workload_name: Name of the workload.
            workload_type: Type of workload. 
            sample_type: The type of sampling technique used. 
        """

        full_feature_file_path = self._global_config.get_block_feature_file_path(workload_type, workload_name)
        if not full_feature_file_path.exists():
            print("Feature file path does not exist for full block trace.")
            return 
        
        with full_feature_file_path.open("r") as full_feature_handle:
            full_trace_feature_dict = load(full_feature_handle)
        
        full_rd_hist_file_path = self._global_config.get_rd_hist_file_path(workload_type, workload_name)
        if not full_rd_hist_file_path.exists():
            print("Feature file path does not exist for full block trace.")
            return 
        
        full_rd_hist = RDHistogram()
        full_rd_hist.load_rd_hist_file(full_rd_hist_file_path)

        r_full_hr_dict, w_full_hr_dict = {}, {}
        wss_block_count = full_trace_feature_dict["wss"]//4096
        for size_split in arange(0.05, 1.0, 0.05):
            size_block_count = int(size_split * wss_block_count)
            r_full_hr_dict[size_split] = full_rd_hist.get_read_hit_rate(size_block_count)
            w_full_hr_dict[size_split] = full_rd_hist.get_write_hit_rate(size_block_count)

        seed_arr, bits_arr, rate_arr = self._sample_config.seed_arr, self._sample_config.bits_arr, self._sample_config.rate_arr
        for seed, bits, rate in product(seed_arr, bits_arr, rate_arr):
            sample_trace_feature_file_path = self._sample_config.get_block_feature_file_path(sample_type, workload_type, workload_name, rate, bits, seed, global_config=self._global_config)
            if not sample_trace_feature_file_path.exists():
                continue 

            rd_hist_file_path = self._sample_config.get_rd_hist_file_path(sample_type, workload_type, workload_name, rate, bits, seed, global_config=self._global_config)
            if not rd_hist_file_path.exists():
                continue 

            percent_diff_feature_file_path = self._sample_config.get_percent_diff_feature_file_path(sample_type, workload_type, workload_name, rate, bits, seed, global_config=self._global_config)
            # if percent_diff_feature_file_path.exists() or not force:
            #     continue 

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
            
            rd_hist = RDHistogram()
            rd_hist.load_rd_hist_file(rd_hist_file_path)

            r_sample_hr_dict, w_sample_hr_dict = {}, {}
            sample_wss_block_count = sample_trace_feature_dict["wss"]//4096
            for sample_size_split in arange(0.05, 1.0, 0.05):
                size_block_count = int(sample_size_split * sample_wss_block_count)
                r_sample_hr_dict[sample_size_split] = rd_hist.get_read_hit_rate(size_block_count)
                w_sample_hr_dict[sample_size_split] = rd_hist.get_write_hit_rate(size_block_count)
            
            for key in r_full_hr_dict:
                percent_diff_stats["r_hr_{}".format(int(100*key))] = 100*(r_full_hr_dict[key] - r_sample_hr_dict[key])/r_full_hr_dict[key]
                percent_diff_stats["w_hr_{}".format(int(100*key))] = 100*(w_full_hr_dict[key] - w_sample_hr_dict[key])/w_full_hr_dict[key]
            print(dumps(percent_diff_stats))
            print("Computed percent diff for files {}, {}".format(sample_trace_feature_file_path, percent_diff_feature_file_path))
            percent_diff_feature_file_path.parent.mkdir(exist_ok=True, parents=True)
            with percent_diff_feature_file_path.open('w+') as percent_diff_file_handle:
                dump(percent_diff_stats, percent_diff_file_handle, indent=2)