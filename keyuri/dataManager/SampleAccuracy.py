from pathlib import Path 
from numpy import mean, quantile


DEFAULT_HRC_ERR_PERCENTILES = [0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999]


def get_absolute_hit_ratio_err(
        full_hrc_arr: list, 
        sample_hrc_arr: list, 
        sampling_rate: float,
        percentile_arr: list = DEFAULT_HRC_ERR_PERCENTILES
) -> dict:
    """ Return statistics about the error in sample hit rate values. 

    Args:
        full_hrc_arr: List of hit rate values from the full trace indexed by cache sizes. 
        sample_hrc_arr: List of hit rate values from the sample trace indexed by cache sizes. 
        sampling_rate: The sampling rate. 
        percentile_arr: List of percentiles of hit rate error values being tracked.
    
    Returns:
        hr_err_dict: Dictionary of hit rate error values. 
    """
    assert sampling_rate > 0.0 and sampling_rate < 1.0, "Sampling rate should be greater than 0.0 and less than 1.0"

    err_arr = []
    max_cache_size = len(full_hrc_arr) - 1
    for sample_size in range(1, len(sample_hrc_arr)):
        full_size = int(sample_size/sampling_rate)
        # sometimes the scaled up cache size might exceed the max cache size of full trace
        # then we just finish, no point trying higher values 
        if full_size > max_cache_size:
            break 

        err_arr.append(abs(full_hrc_arr[full_size] - sample_hrc_arr[sample_size]))

    percentile_val_arr = quantile(err_arr, percentile_arr)
    err_dict = {}
    err_dict["mean"] = mean(err_arr)
    for percentile_val in percentile_val_arr:
        err_dict[percentile_val] = percentile_val_arr
    
    return err_dict 


def get_hit_ratio_curve(rd_hist_df: Path) -> list:
    """ Generate list of hit ratio with cache size as the index given a file containing the reuse
    distance histogram.

    Args:
        rd_hist_df: DataFrame with read and write reuse distance counts. The first row (index=0) contains
                        the request with infinite reuse distance (cold miss). Start from row $n > 0, the
                        row $n will contain the number of read and write requests with reuse distance $n-1. 
    
    Returns:
        hit_ratio_curve: List of hit rate values with cache size as index. The value at hit_ratio_curve[$s] where
                            $s is the size of the cache is the hit ratio value acheivable if we use a cache of size $s.
    """
    rd_hist_row = rd_hist_df.iloc[0]

    read_cold_miss_count, write_cold_miss_count = rd_hist_row[0], rd_hist_row[1]
    total_cold_miss_count = read_cold_miss_count + write_cold_miss_count

    read_cum_rd_hist_count = rd_hist_df.iloc[1:, 0].to_numpy().cumsum()
    write_cum_rd_hist_count = rd_hist_df.iloc[1:, 1].to_numpy().cumsum()
    total_cum_rd_hist_count = rd_hist_df.iloc[1:].sum(axis=1).to_numpy().cumsum()

    total_request_count = total_cold_miss_count + read_cum_rd_hist_count[-1] + write_cum_rd_hist_count[-1]
    read_hr_arr = read_cum_rd_hist_count/total_request_count
    write_hr_arr = write_cum_rd_hist_count/total_request_count
    overall_hr_arr = total_cum_rd_hist_count/total_request_count

    return [0] + list(read_hr_arr), [0] + list(write_hr_arr), [0] + list(overall_hr_arr)