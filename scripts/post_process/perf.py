from pathlib import Path 
from pandas import read_csv

from keyuri.analysis.PostProcessPerf import get_all_pp_perf
from keyuri.analysis.Plotter import scatter_plot, scatter_plot_pp_perf

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
    scatter_plot_pp_perf(perf_df, ERR_GROUP_LIST, output_path, fig_size=[28, 40])

    # for err_pair in ERR_GROUP_LIST:
    #     y_val_arr = []
    #     x_val_arr = []
    #     legend_arr = []
    #     for feature_name in err_pair:
    #         if is_read(feature_name):
    #             legend_arr.append("Read")
    #         else:
    #             legend_arr.append("Write")
        
    #         diff_arr, it_arr = get_diff_df(perf_df, feature_name)
    #         y_val_arr.append(diff_arr)
    #         x_val_arr.append(it_arr)
        

    #     file_name = "-".join(err_pair)
    #     output_path = PATH_DIR.joinpath("{}.png".format(file_name))
    #     if len(legend_arr) <= 1:
    #         legend_arr = []


    #     scatter_plot(x_val_arr, y_val_arr, legend_arr, 'Iteration', 'Delta Error (%)', output_path)
        
    
    # features_to_plot = ['cur_mean_read_size', 
    #                         'cur_mean_write_size', 
    #                         'cur_mean_read_iat', 
    #                         'cur_mean_write_iat', 
    #                         'misalignment_per_read',
    #                         'misalignment_per_write',
    #                         'write_ratio',
    #                         'mean',
    #                         'mean_r_per',
    #                         'mean_w_per',
    #                         'mean_o_per']

    # for cur_feature in features_to_plot:
    #     plt.rcParams.update({'font.size': 30})
    #     fig, ax = plt.subplots(figsize=[28,10])

    #     rate_color_dict = { 5: 'red', 10: 'blue', 20: 'green'}
    #     for group, df in perf_df.groupby(by=["workload", "rate", "bits", "seed"]):
    #         df = df.sort_values(by=["it"])
    #         df["diff"] = df[cur_feature].diff()
    #         df.iloc[0]["diff"] = df.iloc[0][cur_feature]
    #         ax.scatter(df["it"], df["diff"], s=200, alpha=0.7, color='blue')
        
    #     # handles, labels = plt.gca().get_legend_handles_labels()
    #     # by_label = OrderedDict(zip(labels, handles))
    #     # plt.legend(by_label.values(), by_label.keys(), fontsize=45)
        
    #     ax.set_xlabel("Iteration")
    #     ax.set_ylabel("Error Reduced (%)")

    #     ax.axhline(0, linestyle='--', color='red')

    #     #ax.set_xlim(xmax=20000)

    #     plt.tight_layout()
    #     plt.savefig(PATH_DIR.joinpath("{}.png".format(cur_feature)))
    #     plt.close(fig)


if __name__ == "__main__":
    main()