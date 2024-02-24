from pandas import read_csv 

def main():
    df = read_csv("./files/sample_features/features.csv")
    df = df[(df["rate"] <= 40) & (df["percent_read_hr"] < 5)]
    for group_index, group_df in df.groupby(by=["workload", "rate"]):
        print(group_df.sort_values(by=["mean"])[["percent_read_hr", "percent_overall_hr", "mean", "workload", "rate", "bits", "seed"]].iloc[:20].to_string())

if __name__ == "__main__":
    main()