from fractions import Fraction
from pathlib import Path 
from math import ceil 
from pandas import DataFrame, read_csv 

from cydonia.profiler.RDHistogram import RDHistogram
from keyuri.config.BaseConfig import BaseConfig, get_all_cp_workloads


class CumHitRateError:
    def __init__(
            self, 
            sample_set_name: str, 
            cum_hit_rate_error_file: Path,
            dir_config: BaseConfig = BaseConfig()
    ) -> None:
        self._sample_set_name = sample_set_name
        self._dir_config = dir_config
        self._err_file = cum_hit_rate_error_file

        self._err_df = None 
        if self._err_file.exists():
            self._err_df = read_csv(self._err_file)
    

    def check_if_completed(self, workload, rate, bits, seed):
        if not self._err_df:
            return False 
        else:
            rows = self._err_df[(self._err_df["rate"] == rate) &
                                    (self._err_df["bits"] == bits) &
                                    (self._err_df["seed"] == seed) &
                                    (self._err_df["workload"] == workload)]
            return len(rows) > 0
        
    
    def add_to_error_file(self, error_dict: dict) -> None:
        new_df = DataFrame([error_dict])
        if not self._err_df:
            new_df.to_csv(self._err_file, index=False)
        else:
            cur_df = read_csv(self._err_file)
            
    

    def compute_cum_hit_rate_error(self, error_file, workload, rate, bits, seed):
        hit_rate_df = read_csv(error_file)

        # get mean, p99 of hit rate error (read/write/overall)
        percent_diff_dict = {}

        req_type_arr = ["read", "write", "overall"]
        quantiles = [0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999]
        for req_type in req_type_arr:
            cur_df = hit_rate_df["percent_{}_hr".format(req_type)]
            percent_diff_dict["{}_mean".format(req_type)] = cur_df.mean()
            quantile_val_arr = cur_df.quantile(quantiles)
            for quantile_val in quantiles:
                percent_diff_dict["{}_{}".format(req_type, quantile_val)] = quantile_val_arr[quantile_val]

        percent_diff_dict["rate"], percent_diff_dict["bits"], percent_diff_dict["seed"] = rate, bits, seed
        percent_diff_dict["workload"] = workload
        return percent_diff_dict
    

    def generate_all(self):
        hit_rate_error_file_list = self._dir_config.get_all_hit_rate_error_files(self._sample_set_name)
        for hit_rate_error_file in hit_rate_error_file_list:
            workload_name = hit_rate_error_file.parent.name
            rate, bits, seed = self._dir_config.get_sample_file_info(hit_rate_error_file)

            if not self.check_if_completed(workload_name, rate, bits, seed):
                print(hit_rate_error_file)
                percent_diff_dict = self.compute_cum_hit_rate_error(hit_rate_error_file, workload_name, rate, bits, seed)
                print(percent_diff_dict)





class MultiHitRateError:
    def __init__(
            self,
            full_rd_hist_file_path: Path,
            sample_rd_hist_dir_path: Path,
            hit_rate_err_dir_path: Path 
    ) -> None:
        self._full_rd_hist_file_path = full_rd_hist_file_path
        self._sample_rd_hist_dir_path = sample_rd_hist_dir_path
        self._hit_rate_err_dir_path = hit_rate_err_dir_path
    

    def generate_hit_rate_err_files(self) -> None:
        """ Generate hit rate error files for all files pertaining to the workload. """
        for sample_rd_hist_file_path in self._sample_rd_hist_dir_path.iterdir():
            hit_rate_err_file_path = self._hit_rate_err_dir_path.joinpath("{}.csv".format(sample_rd_hist_file_path.stem))
            if hit_rate_err_file_path.exists():
                print("Hit rate error file path {} already exists.".format(hit_rate_err_file_path))
                continue
            
            print("Generating hit rate error file {}.".format(hit_rate_err_file_path))
            hit_rate_err_file_path.touch()
            split_rd_hist_file_name = sample_rd_hist_file_path.stem.split('_')
            rate = float(split_rd_hist_file_name[0])/100

            hit_rate_err = HitRateError(self._full_rd_hist_file_path, sample_rd_hist_file_path, rate)
            hit_rate_err_df = hit_rate_err.get_hit_rate_err_df()
            hit_rate_err_file_path = self._hit_rate_err_dir_path.joinpath(sample_rd_hist_file_path.name)
            hit_rate_err_df.to_csv(hit_rate_err_file_path, index=False)


class HitRateError:
    def __init__(
            self,
            full_rd_hist_file_path: Path,
            sample_rd_hist_file_path: Path,
            sample_rate: float 
    ) -> None:
        self._full_rd_hist_file_path = full_rd_hist_file_path
        self._sample_rd_hist_file_path = sample_rd_hist_file_path 
        self._sample_rate = sample_rate 

        self._full_rd_hist = RDHistogram(-1)
        self._full_rd_hist.load_rd_hist_file(self._full_rd_hist_file_path)

        self._sample_rd_hist = RDHistogram(-1)
        self._sample_rd_hist.load_rd_hist_file(self._sample_rd_hist_file_path)
    

    def get_hit_rate_err_df(self) -> DataFrame:
        """ Get a DataFrame of hit rate error values at various cache sizes.

        Returns:
            hit_rate_err_df: DataFrame of hit rate error values at different cache sizes. 
        """
        full_hr_arr = self._full_rd_hist.get_hit_rate_arr()
        sample_hr_arr = self._sample_rd_hist.get_hit_rate_arr()

        """ Convert the sampling rate from float like 0.8 to a fraction 4/5. The numerator
        and denominator of the fraction denotes the granularity of sample and full reuse
        distance histogram during error calculation. Here, total hit count at cache sizes
        1, 2, 3 and 4 in the sample reuse distance is compared to the total hit count at
        cache sizes 1, 2, 3, 4 and 5 in reuse distances from the full trace. """
        sample_rate_fraction = Fraction(str(self._sample_rate))
        sample_step_size, full_step_size = sample_rate_fraction.numerator, sample_rate_fraction.denominator
        num_sample_size_windows = ceil((self._sample_rd_hist.max_rd+1)/sample_step_size)     

        hit_rate_err_arr = []
        for size_window_index in range(1, num_sample_size_windows+1):
            sample_cache_size = size_window_index * sample_step_size
            full_cache_size = size_window_index * full_step_size

            if sample_cache_size >= len(sample_hr_arr):
                sample_cache_size = len(sample_hr_arr) - 1 
            
            if full_cache_size >= len(full_hr_arr):
                full_cache_size = len(full_hr_arr) - 1 

            sample_overall_hr = sample_hr_arr[sample_cache_size][0]
            sample_read_hr = sample_hr_arr[sample_cache_size][1]
            sample_write_hr = sample_hr_arr[sample_cache_size][2]

            full_overall_hr = full_hr_arr[full_cache_size][0]
            full_read_hr = full_hr_arr[full_cache_size][1]
            full_write_hr = full_hr_arr[full_cache_size][2]

            abs_overall_err = abs(full_overall_hr - sample_overall_hr)
            hr_overall_er = 100 * abs_overall_err/full_overall_hr

            abs_read_err = abs(full_read_hr - sample_read_hr)
            hr_read_err = 100 * abs_read_err/full_read_hr

            abs_write_err = abs(full_write_hr - sample_write_hr)
            hr_write_err = 100 * abs_write_err/full_write_hr

            hit_rate_err_arr.append({"sample_size": sample_cache_size, 
                                        "full_size": full_cache_size,
                                        "sample_overall_hr": sample_overall_hr,
                                        "sample_read_hr": sample_read_hr,
                                        "sample_write_hr": sample_write_hr,
                                        "full_overall_hr": full_overall_hr,
                                        "full_read_hr": full_read_hr,
                                        "full_write_hr": full_write_hr,
                                        "delta_overall_hr": abs_overall_err,
                                        "percent_overall_hr": hr_overall_er,
                                        "delta_read_hr": abs_read_err,
                                        "percent_read_hr": hr_read_err,
                                        "delta_write_hr": abs_write_err,
                                        "percent_write_hr": hr_write_err})

        return DataFrame(hit_rate_err_arr, index=range(len(hit_rate_err_arr)))