""" Plot the feature error per iteration. 
"""

from pathlib import Path 
from pandas import read_csv

from keyuri.analysis.PostProcessPerf import get_all_pp_perf
from keyuri.analysis.Plotter import scatter_plot_pp_perf

PATH_DIR = Path("./files/perf_plots/")
PERF_CSV_FILE = Path("./files/delta_pp_perf.csv")
FORCE_COMPUTE = False 


ERR_GROUP_LIST = [['cur_mean_read_size', 'cur_mean_write_size'],
                    ['cur_mean_read_iat', 'cur_mean_write_iat'],
                    ['misalignment_per_read', 'misalignment_per_write'],
                    ['mean_r_per', 'mean_w_per'],
                    ['write_ratio'],
                    ['mean']]

def is_read(feature_name):
    return '_r_' in feature_name or 'read' in feature_name


def is_write(feature_name):
    return '_w_' in feature_name or 'write' in feature_name


def get_diff_df(perf_df, cur_feature):
    diff_arr = []
    it_arr = []
    for group, df in perf_df.groupby(by=["workload", "rate", "bits", "seed"]):
        df = df.sort_values(by=["it"])
        diff_df = df[cur_feature].diff()
        diff_df.iloc[0] = df.iloc[0][cur_feature]
        diff_arr.extend(diff_df.to_list())
        it_arr.extend(df["it"].to_list())
    assert len(diff_arr) == len(it_arr)
    return diff_arr, it_arr


def main():
    if not PERF_CSV_FILE.exists() or FORCE_COMPUTE:
        perf_df = get_all_pp_perf()
        perf_df.to_csv(PERF_CSV_FILE, index=False)
    else:
        perf_df = read_csv(PERF_CSV_FILE)
    
    output_path = PATH_DIR.joinpath("post_process_perf.pdf")
    scatter_plot_pp_perf(perf_df, ERR_GROUP_LIST, output_path, fig_size=[28, 40], font_size=40)


if __name__ == "__main__":
    main()