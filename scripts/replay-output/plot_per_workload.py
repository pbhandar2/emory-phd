from pathlib import Path 
from pandas import DataFrame
from argparse import ArgumentParser

import matplotlib.pyplot as plt
from matplotlib.axes import Axes

from keyuri.analysis.MultiPlot import plot_replay_bar_plot

from get_replay_output_df import get_replay_err_df



DEFAULT_REPLAY_PERF_MAP = {
    "bandwidth": "Bandwidth",
    "blockReadLatency_avg_ns": "BlockReadLatency\navg",
    "blockWriteLatency_avg_ns": "BlockWriteLatency\navg",
    "blockReadLatency_p99_ns": "BlockReadLatency\np99",
    "blockWriteLatency_p99_ns": "BlockWriteLatency\np99"
}


def plot_per_workload(
        err_df: DataFrame, 
        output_path: Path, 
        bits_arr: list = [0,4]
) -> None:
    data = {}
    for row_index, row in err_df.iterrows():
        for perf_key in DEFAULT_REPLAY_PERF_MAP.keys():
            if perf_key not in data:
                data[perf_key] = {}

            cur_bits = int(row["bits"])
            if cur_bits not in bits_arr:
                continue 

            if cur_bits not in data[perf_key]:
                data[perf_key][cur_bits] = []
            
            perf_metric = row[perf_key]
            data[perf_key][cur_bits].append(perf_metric)
    
    plot_replay_bar_plot(data, output_path)
        

def main():
    parser = ArgumentParser(description="Plot replay performance for a workload.")
    parser.add_argument("-w", "--workload", type=str, help="Name of the workload.")
    parser.add_argument("-r", "--rate", type=int, help="Sampling rate.")
    args = parser.parse_args()

    err_df = get_replay_err_df()
    err_df = err_df[(err_df["workload"] == args.workload) & (err_df["rate"] == args.rate) & (err_df[""])]
    output_path = Path("./files/per_workload_plot/{}_{}.pdf".format(args.workload, args.rate))
    plot_per_workload(err_df, output_path)

    # rate_arr, bits_arr = [10, 20], [0, 4]
    # output_dir = Path("./files/per_workload_plot")
    # output_dir.mkdir(exist_ok=True, parents=True)
    # err_df = err_df[err_df["workload"] == args.workload]
    # for rate in rate_arr:
    #     cur_df = err_df[err_df["rate"] == rate]

if __name__ == "__main__":
    main()