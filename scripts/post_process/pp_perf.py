from enum import Enum
from argparse import ArgumentParser
from pandas import read_csv, DataFrame

from keyuri.config.BaseConfig import BaseConfig


class HrHeaders(Enum):
    o_hr_per = "percent_overall_hr"
    o_hr_delta = "delta_overall_hr"
    r_hr_per = "percent_read_hr"
    r_hr_delta = "delta_read_hr"
    w_hr_per = "percent_write_hr"
    w_hr_delta = "delta_write_hr"


class BlockHeaders(Enum):
    mean_read_size = "cur_mean_read_size"
    mean_write_size = "cur_mean_write_size"
    mean_read_iat = "cur_mean_read_iat"
    mean_write_iat = "cur_mean_write_iat"
    mean_read_misalign = "misalignment_per_read"
    mean_write_misalign = "misalignment_per_write"
    write_ratio = "write_ratio"
    block_count = "block_count"


def get_hr_dict(hr_df: DataFrame):
    hr_dict = {}

    hr_dict["mean_o_delta"] = hr_df[HrHeaders.o_hr_delta.value].mean()
    hr_dict["p90_o_delta"] = hr_df[HrHeaders.o_hr_delta.value].quantile(0.9)
    hr_dict["p99_o_delta"] = hr_df[HrHeaders.o_hr_delta.value].quantile(0.99)

    hr_dict["mean_o_per"] = hr_df[HrHeaders.o_hr_per.value].mean()
    hr_dict["p90_o_per"] = hr_df[HrHeaders.o_hr_per.value].quantile(0.9)
    hr_dict["p99_o_per"] = hr_df[HrHeaders.o_hr_per.value].quantile(0.99)

    hr_dict["mean_r_delta"] = hr_df[HrHeaders.r_hr_delta.value].mean()
    hr_dict["p90_r_delta"] = hr_df[HrHeaders.r_hr_delta.value].quantile(0.9)
    hr_dict["p99_r_delta"] = hr_df[HrHeaders.r_hr_delta.value].quantile(0.99)

    hr_dict["mean_r_per"] = hr_df[HrHeaders.r_hr_per.value].mean()
    hr_dict["p90_r_per"] = hr_df[HrHeaders.r_hr_per.value].quantile(0.9)
    hr_dict["p99_r_per"] = hr_df[HrHeaders.r_hr_per.value].quantile(0.99)

    hr_dict["mean_w_delta"] = hr_df[HrHeaders.w_hr_delta.value].mean()
    hr_dict["p90_w_delta"] = hr_df[HrHeaders.w_hr_delta.value].quantile(0.9)
    hr_dict["p99_w_delta"] = hr_df[HrHeaders.w_hr_delta.value].quantile(0.99)

    hr_dict["mean_w_per"] = hr_df[HrHeaders.w_hr_per.value].mean()
    hr_dict["p90_w_per"] = hr_df[HrHeaders.w_hr_per.value].quantile(0.9)
    hr_dict["p99_w_per"] = hr_df[HrHeaders.w_hr_per.value].quantile(0.99)

    return hr_dict


def main():
    config = BaseConfig()
    sample_set_name = "basic"
    metric_name = "mean"
    algo_bits = 4
    bits = 4 
    seed = 42 

    err_df_dict = {}
    err_dict_list = []
    output_dir = config.get_sample_pp_hit_rate_dir(sample_set_name)
    for file_path in output_dir.rglob("*"):
        if not file_path.is_file():
            continue 

        workload_name = file_path.parent.parent.name
        rate, bits, seed = config.get_sample_file_info(file_path.parent)
        pp_error_file = config.get_sample_post_process_output_file_path(sample_set_name,
                                                                            workload_name,
                                                                            metric_name,
                                                                            algo_bits,
                                                                            rate,
                                                                            bits,
                                                                            seed)
        
        df_key = "{}-{}-{}-{}".format(workload_name, rate, bits, seed)
        if df_key not in err_df_dict:
            err_df = read_csv(pp_error_file)
        else:
            err_df = err_df_dict[df_key]
        
        err_dict = get_hr_dict(read_csv(file_path))
        err_dict["it"] = int(file_path.stem)
        err_dict["rate"] = rate
        err_dict["workload"] = workload_name
        err_dict["bits"] = bits
        err_dict["seed"] = seed 

        for block_feature_name in BlockHeaders:
            err_dict[block_feature_name.value] = err_df.iloc[err_dict["it"] - 1][block_feature_name.value]
        
        err_dict_list.append(err_dict)
    
    df = DataFrame(err_dict_list)
    df.to_csv("./files/pp_perf_data.csv", index=False)


if __name__ == "__main__":
    main()