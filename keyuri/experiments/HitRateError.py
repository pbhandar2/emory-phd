from fractions import Fraction
from pathlib import Path 
from math import ceil 
from pandas import DataFrame

from cydonia.profiler.RDHistogram import RDHistogram


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