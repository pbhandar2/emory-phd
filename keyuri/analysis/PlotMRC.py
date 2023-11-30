from pathlib import Path 
from pandas import read_csv 
from numpy import full
import matplotlib.pyplot as plt


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