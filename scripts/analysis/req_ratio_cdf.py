from pathlib import Path 

from keyuri.analysis.SampleQuality import SampleQualityData

def main():
    sample_quality_data = SampleQualityData()
    plot_dir = Path("./plots/req_ratio_cdf")
    sample_quality_data.plot_all_cdf(plot_dir)


if __name__ == "__main__":
    main()