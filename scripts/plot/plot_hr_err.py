from pathlib import Path 
from json import load 
from pandas import DataFrame 
from scipy.stats import describe

import matplotlib.pyplot as plt


def load_hr_err_df():
    err_dict_arr = []
    sample_hr_err_dir = Path("/research/file_system_traces/cloudphysics_metadata/sample_hr_err")
    for workload_dir in sample_hr_err_dir.iterdir():
        for hr_err_file in workload_dir.iterdir():
            with hr_err_file.open("r") as f:
                err_dict = load(f)
            err_dict["workload"] = workload_dir.name
            file_name_split = hr_err_file.stem.split("_")
            err_dict["rate"], err_dict["bits"], err_dict["seed"] = int(file_name_split[0]), int(file_name_split[1]), int(file_name_split[2])
            err_dict_arr.append(err_dict)
    return DataFrame(err_dict_arr)


def plot(df, plot_path=Path("./files/hr_err/err.pdf")):
    plt.rcParams.update({'font.size': 30})

    metrics = ["per_mean", "per_0.9"]

    fig, ax = plt.subplots(figsize=[14,10])

    x = 1
    bits_x_ticks_val = []
    bits_x_ticks_index = []
    op_x_ticks_val = []
    op_x_ticks_index = []
    for metric_index, cur_metric in enumerate(metrics):

            r_metric_name = "r_{}".format(cur_metric)
            w_metric_name = "w_{}".format(cur_metric)
            o_metric_name = "o_{}".format(cur_metric)

            read_data_0 = df[df["bits"] == 0][r_metric_name].to_numpy()
            read_data_4 = df[df["bits"] == 4][r_metric_name].to_numpy()

            ax.boxplot(read_data_0, positions=[x], showfliers=False, widths=0.8)
            ax.boxplot(read_data_4, positions=[x + 1], showfliers=False, widths=0.8)

            bits_x_ticks_val.append(0)
            bits_x_ticks_index.append(x)
            bits_x_ticks_val.append(4)
            bits_x_ticks_index.append(x+1)

            op_x_ticks_index.append(x+0.5)
            op_x_ticks_val.append("Read")



            print("R")
            print(describe(read_data_0))
            print(describe(read_data_4))


            read_data_0 = df[df["bits"] == 0][w_metric_name].to_numpy()
            read_data_4 = df[df["bits"] == 4][w_metric_name].to_numpy()

            print("W")
            print(describe(read_data_0))
            print(describe(read_data_4))

            x += 2
            ax.boxplot(read_data_0, positions=[x], showfliers=False, widths=0.8)
            ax.boxplot(read_data_4, positions=[x+1], showfliers=False, widths=0.8)


            bits_x_ticks_val.append(0)
            bits_x_ticks_index.append(x)
            bits_x_ticks_val.append(4)
            bits_x_ticks_index.append(x+1)

            op_x_ticks_index.append(x+0.5)
            op_x_ticks_val.append("Write")


            read_data_0 = df[df["bits"] == 0][o_metric_name].to_numpy()
            read_data_4 = df[df["bits"] == 4][o_metric_name].to_numpy()

            print("O")
            print(describe(read_data_0))
            print(describe(read_data_4))

            x += 2
            ax.boxplot(read_data_0, positions=[x], showfliers=False, widths=0.8)
            ax.boxplot(read_data_4, positions=[x+1], showfliers=False, widths=0.8)


            bits_x_ticks_val.append(0)
            bits_x_ticks_index.append(x)
            bits_x_ticks_val.append(4)
            bits_x_ticks_index.append(x+1)

            op_x_ticks_index.append(x+0.5)
            op_x_ticks_val.append("Overall")


            # write_data = df[df["bits"] == bits][w_metric_name].to_numpy()
            # overall_data = df[df["bits"] == bits][o_metric_name].to_numpy()

            #axs[metric_index].boxplot([read_data, write_data, overall_data], positions=range(x, x+3), showfliers=False)
            # x += 1
            # axs[metric_index].boxplot(write_data)
            x += 4

    print(bits_x_ticks_index)

    ax.set_xticks(bits_x_ticks_index, bits_x_ticks_val, fontsize=30)

    sec = ax.secondary_xaxis(location="bottom")
    sec.set_xticks(op_x_ticks_index, labels=["\n{}".format(_) for _ in op_x_ticks_val], fontsize=30)

    # sec = ax.secondary_xaxis(location="bottom")
    # sec.set_xticks([3.5], labels=["\n\n\n{}".format(_) for _ in ["Mean"]])



    ax.set_ylabel("Error (%)", fontsize=30)

    plot_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(plot_path)
    plt.close(fig)



def main():
    hr_err_df = load_hr_err_df()
    ignore_workload_num_arr = list(range(10,20)) + [54]
    ignore_workload_arr = []
    for workload_num in ignore_workload_num_arr:
         ignore_workload_arr.append("w{}".format(workload_num))
    
    hr_err_df = hr_err_df[~hr_err_df["workload"].isin(ignore_workload_arr)]
    print(hr_err_df[["workload", "r_per_mean"]].to_string())
    plot(hr_err_df)
    


if __name__ == "__main__":
    main()