from keyuri.analysis.SampleQuality import SampleQualityData

def main():
    sample_quality_data = SampleQualityData()
    sample_quality_data.plot_all_sample_quality_per_workload()


if __name__ == "__main__":
    main()