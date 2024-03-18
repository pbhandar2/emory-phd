from pathlib import Path 
from pandas import read_csv 
from numpy import mean, quantile


DEFAULT_HRC_ERR_PERCENTILES = [0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999]


def get_sample_abs_hr_err(
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


def hrc_mae(hrc_arr, sample_hrc_arr, sample_rate):
    quantiles_tracked = DEFAULT_HRC_ERR_PERCENTILES
    err_arr = []
    max_cache_size = 0
    for cur_size in range(1, len(sample_hrc_arr)):
        sample_size = int(cur_size/sample_rate)

        hrc_size = sample_size 
        if sample_size >= len(hrc_arr):
            break

        if hrc_arr[hrc_size] == 0:
            continue 

        hr_diff = abs(hrc_arr[hrc_size] - sample_hrc_arr[cur_size])
        if hr_diff > 0:
            percent_err = 100*(hr_diff)/hrc_arr[hrc_size]
        else:
            percent_err = 0
        max_cache_size = hrc_size
        err_arr.append(percent_err)
    
    percent_diff_dict = {}
    quantile_val_arr = quantile(err_arr, quantiles_tracked)
    for quantile_val in quantiles_tracked:
        percent_diff_dict[quantile_val] = quantile_val_arr[quantile_val]

    return float(mean(err_arr)), max_cache_size


def get_hrc(rd_hist_file_path: Path) -> tuple:
    # get read, write and overall HRC 
    rd_hist_df = read_csv(rd_hist_file_path, names=['r', 'w'])
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