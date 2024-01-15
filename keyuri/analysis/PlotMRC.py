from pathlib import Path 
from pandas import read_csv 
from numpy import mean 


def hrc_mae(hrc_arr, sample_hrc_arr, sample_rate):
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
    return float(mean(err_arr)), max_cache_size


def get_hrc(rd_hist_file_path: Path):
    rd_hist_df = read_csv(rd_hist_file_path, names=['r', 'w'])
    rd_hist_row = rd_hist_df.iloc[0]

    read_cold_miss_count, write_cold_miss_count = rd_hist_row[0], rd_hist_row[1]
    total_cold_miss_count = read_cold_miss_count + write_cold_miss_count

    read_cum_rd_hist_count = rd_hist_df.iloc[1:, 0].to_numpy().cumsum()
    write_cum_rd_hist_count = rd_hist_df.iloc[1:, 1].to_numpy().cumsum()
    total_cum_rd_hist_count = rd_hist_df.iloc[1:].sum(axis=1).to_numpy().cumsum()

    total_request_count = total_cold_miss_count + read_cum_rd_hist_count[-1] + write_cum_rd_hist_count[-1]
    read_hr_arr = 100*read_cum_rd_hist_count/total_request_count
    write_hr_arr = 100*write_cum_rd_hist_count/total_request_count
    overall_hr_arr = 100*total_cum_rd_hist_count/total_request_count

    return [0] + list(read_hr_arr), [0] + list(write_hr_arr), [0] + list(overall_hr_arr)