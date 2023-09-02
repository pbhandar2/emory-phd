"""This class manages metadata generated during creation and profiling of samples and full block traces. 

Usage:
    sample_metadata = SampleMetadata("w09")
    sample_metadata.print_best_samples()
"""

from numpy import array, arange 
from pathlib import Path 
from json import load as json_load 
from numpy import mean 
from pandas import DataFrame

from cydonia.profiler.RDHistogram import RDHistogram
from keyuri.config.Config import GlobalConfig, SampleExperimentConfig


class SampleMetadata:
    def __init__(
            self,
            workload_name: str, 
            workload_type: str = "cp",
            sample_type: str = "iat",
            global_config: GlobalConfig = GlobalConfig(),
            sample_config: SampleExperimentConfig = SampleExperimentConfig()
    ) -> None:
        """This class manages metadata generated during creation and profiling of samples and full block traces. 

        Args:
            workload_name: Name of the workload.
            workload_type: Type of workload.
            sample_type: Type of sample. 
            global_config: Global configuration object. 
            sample_config: Sampling configuration object. 
        
        Attributes:
            _workload_name: Name of the workload.
            _workload_type: Type of workload.
            _sample_type: Type of sample. 
            _global_config: Global configuration object. 
            _sample_config: Sampling configuration object. 
        """
        self._workload_name = workload_name 
        self._workload_type = workload_type 
        self._sample_type = sample_type
        self._global_config = global_config
        self._sample_config = sample_config
    

    def get_cache_feature_dict(
            self, 
            file_path: Path,
            wss_byte: int,
            block_size_byte: int = 4096,
            cache_size_ratio_arr: array = arange(0, 1.01, 0.05)
    ) -> dict: 
        """Get a dictionary of features from a block feature file path. 

        Args:
            file_path: Path to block feature file path. 
        
        Returns:
            feature_dict: Dictionary of block features. 
        """
        rd_hist = RDHistogram()
        rd_hist.load_rd_hist_file(file_path)
        cache_feature_dict = {}
        wss_block = int(wss_byte//block_size_byte) + 1 
        for cache_size_ratio in cache_size_ratio_arr:
            cache_feature_dict["hr_{}".format(cache_size_ratio)] = rd_hist.get_read_hit_rate(int(wss_block*cache_size_ratio))
        return cache_feature_dict
    

    def get_sample_params(
            self, 
            file_path: Path 
    ) -> tuple:
        """Get the sample parameters.
        Args:
            file_path: Path to block feature file path. 
        
        Returns:
            sample_params: Tuple of (rate, bit, seed) sample params. 
        """            
        try:
            split_file_name = file_path.stem.split("_")
            rate, bit, seed = int(split_file_name[0]), int(split_file_name[1]), int(split_file_name[2])
        except ValueError:
            rate, bit, seed = 0, 0, 0
        return rate, bit, seed 


    def get_block_feature_dict(
            self, 
            file_path: Path
    ) -> dict:
        """Get a dictionary of features from a block feature file path. 

        Args:
            file_path: Path to block feature file path. 
        
        Returns:
            feature_dict: Dictionary of block features. 
        """
        with file_path.open('r') as file_handle:
            feature_dict = json_load(file_handle)

        rate, bit, seed = self.get_sample_params(file_path)
        feature_dict["rate"], feature_dict["bit"], feature_dict["seed"] = rate, bit, seed 
        return feature_dict
    

    def get_cum_feature_dict(
            self,
            block_feature_file_path: Path, 
            cache_feature_file_path: Path 
    ) -> dict:
        """Get a dictionary of cumulative features (block and cache). 

        Returns:
            feature_dict: Dictionary of cumulative features. 
        
        Raises:
            AssertionError: Raised if there are overlapping keys in block and cache feature names. 
        """
        block_feature_dict = self.get_block_feature_dict(block_feature_file_path)
        cache_feature_dict = self.get_cache_feature_dict(cache_feature_file_path, block_feature_dict['wss'])
        assert not list(set(block_feature_dict.keys()).intersection(cache_feature_dict.keys())), \
            "There are overlapping keys in block and cache feature dictionary."
        
        return dict(list(block_feature_dict.items()) + list(cache_feature_dict.items()))
        
        
    def get_feature_df(self) -> DataFrame:
        """Get a DataFrame of features of the full block trace and its samples. 

        Returns:
            feature_df: DataFrame of features of the full block trace and its samples. 
        """
        overall_feature_arr = []
        full_trace_block_feature_file_path = self._global_config.get_block_feature_file_path(self._workload_type, self._workload_name)
        full_trace_cache_feature_file_path = self._global_config.get_rd_hist_file_path(self._workload_type, self._workload_name)
        feature_dict = self.get_cum_feature_dict(full_trace_block_feature_file_path, full_trace_cache_feature_file_path)
        overall_feature_arr.append(feature_dict)

        sample_block_feature_dir_path = self._global_config.sample_block_feature_dir_path.joinpath(self._sample_type, self._workload_type, self._workload_name)
        for sample_block_feature_file_path in sample_block_feature_dir_path.iterdir():
            rate, bits, seed = self.get_sample_params(sample_block_feature_file_path)
            sample_trace_cache_feature_file_path = self._sample_config.get_rd_hist_file_path(self._sample_type, 
                                                                                                    self._workload_type, 
                                                                                                    self._workload_name, 
                                                                                                    rate,
                                                                                                    bits, 
                                                                                                    seed,
                                                                                                    global_config=self._global_config)
            print("Rate: {}, Seed: {}, Bits: {}, Sample RD hist: {}, Block feature: {}".format(rate, seed, bits, sample_trace_cache_feature_file_path, sample_block_feature_file_path))
            feature_dict = self.get_cum_feature_dict(sample_block_feature_file_path, sample_trace_cache_feature_file_path)
            overall_feature_arr.append(feature_dict)
        return DataFrame(overall_feature_arr)


    def generate_cumulative_feature_file(self, file_path: Path) -> DataFrame:
        cumulative_features_df = self.get_feature_df()
        cumulative_features_df.to_csv(file_path, index=False)


    def load_percent_diff_df(self) -> DataFrame:
        """Get a DataFrame of percent difference in features of the full block trace and its samples. 

        Returns:
            feature_df: DataFrame of features of the full block trace and its samples. 
        """
        percent_diff_array = []
        sample_percent_diff_dir_path = self._global_config.sample_percent_diff_feature_dir_path.joinpath(self._sample_type, self._workload_type, self._workload_name)
        for percent_diff_file in sample_percent_diff_dir_path.iterdir():
            rate, bit, seed = percent_diff_file.stem.split("_")
            with percent_diff_file.open('r') as percent_diff_file_handle:
                percent_diff_dict = json_load(percent_diff_file_handle)
            percent_diff_dict['rate'] = rate 
            percent_diff_dict['bit'] = bit 
            percent_diff_dict['seed'] = seed 
            percent_diff_dict['mean_percent_hit_rate_error'] = self.get_mean_hit_rate_error(percent_diff_dict)
            percent_diff_dict['mean_overall_error'] = mean([abs(percent_diff_dict['mean_percent_hit_rate_error']),
                                                            abs(percent_diff_dict["iat_avg"]),
                                                            abs(percent_diff_dict["read_size_avg"]),
                                                            abs(percent_diff_dict["write_size_avg"]),
                                                            abs(percent_diff_dict["jd_avg"])])
            percent_diff_array.append(percent_diff_dict)
        return DataFrame(percent_diff_array)


    def print_best_samples(self):
        """Print the percent difference. 
        """
        df = self.load_percent_diff_df().sort_values(by=["mean_overall_error"])
        for rate, group_df in df.groupby(by=["rate"]):
            print(group_df[["bit", "rate", "seed", "mean_overall_error", "iat_avg", "read_size_avg", "write_size_avg", 'mean_percent_hit_rate_error', "jd_avg"]].to_string(index=False))

            
    @staticmethod
    def get_mean_hit_rate_error(percent_diff_dict):
        """Get the mean hit rate from a dictionary of percent difference of features. 

        Args:
            percent_diff_dict: Dictionary of features with percent difference. 
        
        Return:
            mean_hit_rate_error: Mean error in hit rate values. 
        """
        hit_rate_arr = []
        for feature_name in percent_diff_dict:
            if "hr_" not in feature_name:
                continue 
            hit_rate_arr.append(percent_diff_dict[feature_name])
        return mean(hit_rate_arr)