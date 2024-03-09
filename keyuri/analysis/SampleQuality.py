"""Load sample quality data and generate plots.

Plots:
1. sample size ratio vs sampling rate for a workload

"""
from pathlib import Path 
from pandas import read_csv, DataFrame
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np 
import scipy


DEFAULT_DATA_DIR = Path("/research2/mtc/cp_traces/pranav-phd/cp/sampling_features/")
DEFAULT_SAMPLE_QUALITY_PER_WORKLOAD_PLOT_DIR = Path.cwd().joinpath("plots/sample_quality_per_workload")


class SampleQualityData:
    def __init__(
            self, 
            data_dir: Path = DEFAULT_DATA_DIR
    ) -> None:
        self._df = None 
        self._data_dir = data_dir
    

    def get_data_per_workload(self) -> dict:
        data_dict = {}
        for data_path in self._data_dir.rglob("*"):
            workload_name = data_path.stem 
            data_dict[workload_name] = read_csv(data_path)
        return data_dict 


    def get_all_sample_ratio_per_bits_and_rate(self) -> dict:
        data_dict = {}
        per_workload_data = self.get_data_per_workload()
        for workload in per_workload_data:
            df = per_workload_data[workload]
            for (rate, bits), group_df in df.groupby(["rate", "bits"]):
                if rate not in data_dict:
                    data_dict[rate] = {}

                if bits not in data_dict[rate]:
                    data_dict[rate][bits] = []
                
                for req_ratio in group_df["req_ratio"]:
                    #data_dict[rate][bits].append(abs((rate/100)-req_ratio))
                    data_dict[rate][bits].append(req_ratio)
        return data_dict 
    

    def plot_cdf(self, data_arr, plot_path, rate_arr):
        plt.rcParams.update({'font.size': 37})
        fig, ax = plt.subplots(figsize=[28,10])
        ax.violinplot(data_arr)
        ax.scatter(range(1, len(rate_arr)+1), [_/100 for _ in rate_arr], color='red', s=120)
        ax.set_xlabel("Sampling Ratio")
        ax.set_ylabel("Request Ratio")
        ax.set_xticks(range(1, len(rate_arr)+1), [_/100 for _ in rate_arr])
        plt.tight_layout()
        plt.savefig(plot_path)
        plt.close(fig)


    def plot_all_cdf(self, plot_dir: Path):
        data_dict = self.get_all_sample_ratio_per_bits_and_rate()
        rate_arr = [1, 5, 10, 20, 40, 80]
        for bits in [0, 1, 2, 4]:
            data_arr = []
            for rate in rate_arr:
                data_arr.append(data_dict[rate][bits])
            
            plot_path = plot_dir.joinpath("{}.pdf".format(bits))
            self.plot_cdf(data_arr, plot_path, rate_arr)


    def plot_all_sample_quality_per_workload(
            self, 
            plot_dir: Path = DEFAULT_SAMPLE_QUALITY_PER_WORKLOAD_PLOT_DIR
    ) -> None:
        plot_dir.mkdir(exist_ok=True, parents=True)
        data_dict = self.get_data_per_workload()
        for workload_name in data_dict:
            output_path = plot_dir.joinpath("{}.png".format(workload_name))
            self.plot_sample_quality_per_workload(data_dict[workload_name], output_path)
    

    def plot_sample_quality_per_workload(self, quality_df: DataFrame, plot_path: Path) -> None:
        plt.rcParams.update({'font.size': 37})
        fig, ax = plt.subplots(figsize=[28,10])

        marker_dict = {
            0: "D",
            1: "X",
            2: "o",
            4: "s"
        }
        rate_arr = None 
        for group_index, group_df in quality_df.groupby(by=["bits"]):
            sorted_df = group_df.sort_values(by=["rate"])
            rate_arr = sorted_df["rate"]
            req_ratio_arr = sorted_df["req_ratio"]
            ax.scatter(rate_arr, req_ratio_arr, marker=marker_dict[group_index], s=250, alpha=0.6, label=group_index)


        ax.plot([0, 80], [0, 0.8], '--', color='black', alpha=0.4, linewidth=3)

        ax.set_xticks(rate_arr)
        ax.set_xlabel("Sampling Rate (%)")
        ax.set_ylabel("Request Count Ratio")
        ax.legend()
        plt.tight_layout()
        plt.savefig(plot_path)
        plt.close(fig)