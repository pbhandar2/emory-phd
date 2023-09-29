from enum import Enum
from pathlib import Path 
from numpy import genfromtxt, sum, cumsum, array, ndarray, arange, min, max, mean, std, percentile, zeros

from cydonia.profiler.CacheTraceProfiler import CacheTraceProfiler


class ProfileCacheTrace:
    def __init__(
            self,
            cache_trace_path: Path 
    ) -> None:
        self._path = cache_trace_path
    

    def create_rd_hist_file(
            self,
            rd_hist_file_path: Path 
    ) -> None:
        profiler = CacheTraceProfiler(str(self._path.absolute()))
        rd_hist_file_path.parent.mkdir(exist_ok=True, parents=True)
        profiler.create_rd_hist_file(rd_hist_file_path)



class HRCType(Enum):
    READ = 1
    OVERALL = 2
    READ_ONLY = 3



class ProfileRDHistogram:
    def __init__(
            self,
            rd_hist_file_path: Path 
    ) -> None:
        self._path = rd_hist_file_path
        self._hist = genfromtxt(rd_hist_file_path, delimiter=",")
        self._cum_hist = cumsum(self._hist[1:], axis=0)
        self._total_req = sum(self._hist, axis=0)
    

    def get_read_hit_rate(
            self,
            size_blk: int 
    ) -> float:
        assert size_blk > 0, "Size needs to be greater than 0."
        return self._cum_hist[size_blk-1][0]/sum(self._total_req)
    

    def get_read_only_hit_rate(
            self,
            size_blk: int 
    ) -> float:
        assert size_blk > 0, "Size needs to be greater than 0."
        return self._cum_hist[size_blk-1][0]/self._total_req[0]


    def get_hit_rate(
            self,
            size_blk: int 
    ) -> float:
        assert size_blk > 0, "Size needs to be greater than 0."
        return sum(self._cum_hist[size_blk-1])/sum(self._total_req)
    

    def get_hrc(
            self,
            hrc_type=HRCType.READ
    ) -> ndarray:
        hrc_arr = zeros(len(self._cum_hist), dtype=float)
        for cur_size_blk in range(1, len(self._cum_hist)):
            if hrc_type == HRCType.READ:
                hrc_arr[cur_size_blk] = self.get_read_hit_rate(cur_size_blk)
            elif hrc_type == HRCType.OVERALL:
                hrc_arr[cur_size_blk] = self.get_hit_rate(cur_size_blk)
            elif hrc_type == HRCType.READ_ONLY:
                hrc_arr[cur_size_blk] = self.get_read_only_hit_rate(cur_size_blk)
            else:
                raise ValueError("Unrecognized HRC type {}.".format(hrc_type))
        return hrc_arr
    

def get_hrc_err_dict(
        full_hrc_arr: ndarray,
        sample_hrc_arr: ndarray,
        rate: int, 
        cache_size_percent_arr: ndarray = arange(101, dtype=int),
        err_percentile_arr:ndarray = arange(0, 101, step=10, dtype=int)
) -> dict:
    hrc_err_arr = get_hrc_err_arr(full_hrc_arr, sample_hrc_arr, rate, cache_size_percent_arr=cache_size_percent_arr)
    err_dict = { "mean": float(mean(hrc_err_arr[1:])), "std": float(std(hrc_err_arr[1:])) }
    for percentile_value in err_percentile_arr:
        err_dict["err_{}".format(percentile_value)] = float(percentile(hrc_err_arr[1:], percentile_value))
    return err_dict 


def get_hrc_err_arr(
        full_hrc_arr: ndarray,
        sample_hrc_arr: ndarray,
        rate: int,
        cache_size_percent_arr: ndarray = arange(101, dtype=int)
) -> ndarray:
    assert len(full_hrc_arr) >= len(sample_hrc_arr), "Sample HRC has more items than the full HRC."
    percent_hit_rate_error_arr = zeros(len(cache_size_percent_arr)+1, dtype=float)
    for index, cache_size_percent in enumerate(cache_size_percent_arr):
        full_cache_size_blk = int(cache_size_percent*len(full_hrc_arr)/100)
        sample_cache_size_blk = int(cache_size_percent*len(sample_hrc_arr)/100)
        if len(sample_hrc_arr) <= sample_cache_size_blk:
            sample_cache_size_blk = len(sample_hrc_arr)-1

        full_hit_rate = full_hrc_arr[full_cache_size_blk] if full_cache_size_blk < len(full_hrc_arr) else full_hrc_arr[-1]
        sample_hit_rate = sample_hrc_arr[sample_cache_size_blk] if sample_cache_size_blk < len(sample_hrc_arr) else sample_hrc_arr[-1]
        percent_hit_rate_error = abs(100 * (full_hit_rate - sample_hit_rate)/full_hit_rate) if full_hit_rate > 0 else 0.0
        percent_hit_rate_error_arr[index] = percent_hit_rate_error
    
    return percent_hit_rate_error_arr