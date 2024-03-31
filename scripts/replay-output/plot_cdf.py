import matplotlib.pyplot as plt
from matplotlib.axes import Axes
import scipy
from numpy import arange 
import pandas as pd 

from get_replay_output_df import get_replay_err_df


def main():
    err_df = get_replay_err_df()
    err_df = err_df[err_df["workload"] == "w96"]

    output_path = "./files/cdf_set/cdf.pdf"
    plt.rcParams.update({'font.size': 30})
    fig, ax = plt.subplots(figsize=[28,10])

    ax.axhline(10, color='green', linestyle='--')
    #ax.axhline(15, color='blue', linestyle='--')

    # 20_4_42 for w96 no preprocess
    band1_arr = sorted(err_df[(err_df["rate"]==20) & (err_df["bits"]==4) & (err_df["pp_n"] == -1)]["bandwidth"])
    err1_df = err_df[(err_df["rate"]==20) & (err_df["bits"]==4) & (err_df["pp_n"] == -1)][["full_t1_size", "full_t2_size", "t1_size", "t2_size", "rate", "bandwidth"]].sort_values(by=["t1_size", "t2_size"])

    # calculate the proportional values of samples
    p = 1. * arange(len(band1_arr)) / (len(band1_arr) - 1)
    ax.plot(p, band1_arr, '-o', label="20%", markersize=30, alpha=0.6)

    # 10_4_42 for w96 no preprocess
    band2_arr = sorted(err_df[(err_df["rate"]==10) & (err_df["bits"]==4) & (err_df["pp_n"] == -1)]["bandwidth"])
    err2_df = err_df[(err_df["rate"]==10) & (err_df["bits"]==4) & (err_df["pp_n"] == -1)][["full_t1_size", "full_t2_size", "t1_size", "t2_size", "rate", "bandwidth"]].sort_values(by=["t1_size", "t2_size"])

    # calculate the proportional values of samples
    p = 1. * arange(len(band2_arr)) / (len(band2_arr) - 1)
    ax.plot(p, band2_arr, '-*', label="10%", markersize=30, alpha=0.6)

    # 20 process from no preprocess
    band3_arr = sorted(err_df[(err_df["rate"]==10) & (err_df["bits"]==4) & (err_df["pp_n"] > 0)]["bandwidth"])
    err3_df = err_df[(err_df["rate"]==10) & (err_df["bits"]==4) & (err_df["pp_n"] > 0)][["full_t1_size", "full_t2_size", "t1_size", "t2_size", "rate", "bandwidth"]].sort_values(by=["t1_size", "t2_size"])

    # calculate the proportional values of samples
    p = 1. * arange(len(band3_arr)) / (len(band3_arr) - 1)
    ax.plot(p, band3_arr, '-^', label="10% + PP", markersize=30, alpha=0.6)

    # 20 process from no preprocess
    band4_arr = sorted(err_df[(err_df["rate"]==40) & (err_df["bits"]==4) & (err_df["pp_n"] == -1)]["bandwidth"])
    err4_df = err_df[(err_df["rate"]==40) & (err_df["bits"]==4) & (err_df["pp_n"] == -1)][["full_t1_size", "full_t2_size", "t1_size", "t2_size", "rate", "bandwidth"]].sort_values(by=["t1_size", "t2_size"])

    # calculate the proportional values of samples
    p = 1. * arange(len(band4_arr)) / (len(band4_arr) - 1)
    ax.plot(p, band4_arr, '-d', label="40%", markersize=30, alpha=0.6)

    ax.set_ylabel("Percent Bandwidth Error (%)", fontsize=40)
    ax.set_xlabel("CDF", fontsize=40)
    ax.legend(fontsize=35)

    #ax.plot(sorted(band1_arr), per_20_pp_w96_cdf)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)

    print(err1_df)
    print(err2_df)
    print(err3_df)
    print(err4_df)





if __name__ == "__main__":
    main()