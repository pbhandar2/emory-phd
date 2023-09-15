"""Scatter plot (specified sample rate, effective sample rate) with marker indicating bits
and then a line x=y. 

Usage:
    python3 plot_specified_vs_eff_rate.py 
"""

from collections import OrderedDict
from glob import glob 
from pathlib import Path 
from argparse import ArgumentParser
from json import load, dumps

import matplotlib.pyplot as plt

from keyuri.config.Config import GlobalConfig
from keyuri.analysis.SampleMetadata import SampleMetadata


def main():
    global_config = GlobalConfig()
    sample_feature_dir_path = global_config.sample_split_feature_dir_path
    plot_data = {}
    for metadata_file_path in glob("{}/**/**/**/**".format(str(sample_feature_dir_path))):
        rate, bits, seed = Path(metadata_file_path).stem.split("_")

        if bits not in plot_data:
            plot_data[bits] = []
        
        with Path(metadata_file_path).open("r") as metadata_file_handle:
            metadata_dict = load(metadata_file_handle)

        specified_rate = 100*metadata_dict["rate"]
        eff_sample_rate = 100.0*metadata_dict["sampled_lba_count"]/(metadata_dict["sampled_lba_count"] + metadata_dict["not_sampled_lba_count"])
        plot_data[bits].append([int(100*metadata_dict["rate"]), 100.0*(specified_rate-eff_sample_rate)/specified_rate])
    
    print(dumps(plot_data, indent=2))
    plt.rcParams.update({'font.size': 22})
    fig, ax = plt.subplots(figsize=[10,7])
    marker_arr = ["*", "o", "^", "p", "d", "x", "8"]
    color_arr = ['r', 'g', 'b', 'black', 'orange', 'brown', 'purple']
    bits_index = 0 
    for bits in plot_data:
        for rate, eff_rate in plot_data[bits]:
            print(rate, eff_rate)
            ax.scatter(rate, eff_rate, marker=marker_arr[bits_index], label=bits, alpha=0.5, s=90, color=color_arr[bits_index])
        bits_index += 1
    #plt.axline((0,0),slope=1, linestyle='--', alpha=0.6)
    xtick_arr = [1,5,10,20,40,80]
    ax.set_xticks(xtick_arr)
    ax.set_xticklabels([str(_) for _ in xtick_arr])
    ax.set_xlabel("Specified Rate (%)")
    ax.set_ylabel("Percent Difference in Specifed and Effective Sampling Rate")

    #handles, labels = plt.gca().get_legend_handles_labels()
    #by_label = OrderedDict(zip(labels, handles))


    handles, labels = ax.get_legend_handles_labels()
    # sort both labels and handles by labels
    labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: int(t[0])))
    by_label = OrderedDict(zip(labels, handles))

    plt.legend(by_label.values(), by_label.keys(), bbox_to_anchor=[0.85, 0.95])
    plt.tight_layout()
    plt.savefig("./files/specified_vs_eff_rate/plot.png")
        
        


if __name__ == "__main__":
    main()