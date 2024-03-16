from pathlib import Path 

from keyuri.analysis.MultiViolinPlot import load_sample_error_data, multi_bar_feature_error_plot, plot_multi_level_bar_plot


def main():
    # feature_name = "cur_mean_read_size"
    # error_data = load_sample_error_data(feature_name)
    # output_file_path = Path("plots/multi_violin_sample_error").joinpath("{}.pdf".format(feature_name))
    # multi_bar_feature_error_plot(error_data, output_file_path)

    overall_file_path = Path("plots/multi_violin_sample_error").joinpath("all.pdf")
    plot_multi_level_bar_plot(overall_file_path)



if __name__ == "__main__":
    main()