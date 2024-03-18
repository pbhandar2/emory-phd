from pathlib import Path 

from keyuri.analysis.MultiPlot import plot_multi_level_box_plot


def main():
    overall_file_path = Path("plots/multi_violin_sample_error").joinpath("block.pdf")
    plot_multi_level_box_plot(overall_file_path)

    print("Plotted the block!")
    overall_file_path = Path("plots/multi_violin_sample_error").joinpath("hr.pdf")
    plot_multi_level_box_plot(overall_file_path, plot_type="hr")



if __name__ == "__main__":
    main()