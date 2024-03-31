from pathlib import Path 

from keyuri.analysis.SampleQuality import SampleQualityData

def main():
    sample_quality_data = SampleQualityData()
    plot_dir = Path("./plots/req_ratio_cdf")
    #sample_quality_data.plot_all_cdf(plot_dir)
    sample_quality_data.plot_all_cdf_sub_plot_in_one(Path("./plots/req_ratio_cdf/req_sample_ratio.pdf"))


if __name__ == "__main__":
    main()