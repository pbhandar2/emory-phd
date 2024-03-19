import matplotlib.pyplot as plt
from matplotlib.axes import Axes
import scipy
from numpy import arange 

from get_replay_output_df import get_replay_err_df


def main():
    err_df = get_replay_err_df()
    err_df = err_df[err_df["workload"] == "w96"]

    output_path = "./files/cdf_set/cdf.pdf"
    plt.rcParams.update({'font.size': 30})
    fig, ax = plt.subplots(figsize=[28,10])


    print(err_df)

    # 20_4_42 for w96 no preprocess
    set1_df = sorted(err_df[(err_df["rate"]==20) & (err_df["bits"]==4) & (err_df["pp_n"] == -1)]["bandwidth"])

    # calculate the proportional values of samples
    p = 1. * arange(len(set1_df)) / (len(set1_df) - 1)
    ax.plot(p, set1_df, '-o', label="20%", markersize=30, alpha=0.6)

    # 10_4_42 for w96 no preprocess
    set2_df = sorted(err_df[(err_df["rate"]==10) & (err_df["bits"]==4) & (err_df["pp_n"] == -1)]["bandwidth"])

    # calculate the proportional values of samples
    p = 1. * arange(len(set2_df)) / (len(set2_df) - 1)
    ax.plot(p, set2_df, '-*', label="10%", markersize=30, alpha=0.6)

    # 20 process from no preprocess
    set3_df = sorted(err_df[(err_df["rate"]==10) & (err_df["bits"]==4) & (err_df["pp_n"] > 0)]["bandwidth"])

    # calculate the proportional values of samples
    p = 1. * arange(len(set3_df)) / (len(set3_df) - 1)
    ax.plot(p, set3_df, '-^', label="10% + PP", markersize=30, alpha=0.6)

    # 20 process from no preprocess
    set4_df = sorted(err_df[(err_df["rate"]==40) & (err_df["bits"]==4) & (err_df["pp_n"] == -1)]["bandwidth"])

    # calculate the proportional values of samples
    p = 1. * arange(len(set4_df)) / (len(set4_df) - 1)
    ax.plot(p, set4_df, '-d', label="40%", markersize=30, alpha=0.6)

    ax.set_ylabel("Percent Bandwidth Error (%)", fontsize=40)
    ax.set_xlabel("CDF", fontsize=40)
    ax.legend(fontsize=35)


    #ax.plot(sorted(set1_df), per_20_pp_w96_cdf)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)





if __name__ == "__main__":
    main()