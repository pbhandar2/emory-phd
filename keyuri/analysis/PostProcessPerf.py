from pathlib import Path 
from enum import Enum
from pandas import read_csv, DataFrame
from json import load, dump, loads
from numpy import mean 

from keyuri.config.BaseConfig import BaseConfig
from keyuri.analysis.ProfileSampleCacheTrace import get_sample_blk_err

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


def get_mean_err(blk_err_dict: dict, mean_r_per_err: float, mean_w_per_err: float):
    err_list = [mean_r_per_err, mean_w_per_err]
    for key in BlockHeaders:
        err_list.append(blk_err_dict[key.value])
    return mean(err_list)


def get_blk_diff(init_blk_err_dict: dict, pp_blk_err_df: DataFrame) -> dict:
    diff_dict = {}
    for key in BlockHeaders:
        diff_dict[key.value] = init_blk_err_dict[key.value] - pp_blk_err_df[key.value]
    return diff_dict


def get_hr_diff(hr1_stat: dict, hr2_stat: dict) -> dict:
    diff_dict = {}
    for key in hr1_stat:
        diff_dict[key] = hr1_stat[key] - hr2_stat[key]
    return diff_dict


def get_hr_stat_dict(hr_df: DataFrame) -> dict:
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


def get_all_pp_perf(
        sample_set_name: str = "basic",
        metric_name: str = "mean",
        algo_bits: int = 4,
        config: BaseConfig = BaseConfig()
) -> None:
    # iterate through each post-process hit rate err file 
    final_perf_dict_arr = []
    output_dir = config.get_sample_pp_hit_rate_dir(sample_set_name)
    for pp_hr_err_file in output_dir.rglob("*"):
        if not pp_hr_err_file.is_file():
            continue 

        hr_err_iteration = int(pp_hr_err_file.stem)
        workload_name = pp_hr_err_file.parent.parent.name
        rate, bits, seed = config.get_sample_file_info(pp_hr_err_file.parent)

        init_hr_err_file = config.get_hit_rate_error_file_path(sample_set_name,
                                                                workload_name,
                                                                rate,
                                                                bits,
                                                                seed)
        
        # difference between the init and current hr error 
        init_hr_err_df = read_csv(init_hr_err_file)
        pp_hr_err_df = read_csv(pp_hr_err_file)
        init_hr_err_dict = get_hr_stat_dict(init_hr_err_df)
        pp_hr_err_dict = get_hr_stat_dict(pp_hr_err_df)
        hr_diff_dict = get_hr_diff(init_hr_err_dict, pp_hr_err_dict)

        pp_blk_err_file = config.get_sample_post_process_output_file_path(sample_set_name,
                                                                            workload_name,
                                                                            metric_name,
                                                                            algo_bits,
                                                                            rate,
                                                                            bits,
                                                                            seed)
        
        init_blk_err_file = config.get_sample_block_error_file_path(sample_set_name,
                                                                        workload_name,
                                                                        rate,
                                                                        bits,
                                                                        seed)
        
        # create sample error file if it does not exist 
        if not init_blk_err_file.exists():
            blk_err_dict = get_sample_blk_err(workload_name, rate, bits, seed)
            init_blk_err_file.parent.mkdir(exist_ok=True, parents=True)
            with init_blk_err_file.open("w+") as f:
                dump(blk_err_dict, f)

        # difference between init and current blk error 
        with init_blk_err_file.open("r") as f:
            init_blk_err_dict = load(f)

        pp_blk_err_df = read_csv(pp_blk_err_file)
        pp_blk_err_dict = loads(pp_blk_err_df.iloc[hr_err_iteration - 1].to_json())
        blk_diff_dict = get_blk_diff(init_blk_err_dict, pp_blk_err_dict)

        # get the init mean error and pp mean error 
        init_mean_err = get_mean_err(init_blk_err_dict, init_hr_err_dict["mean_r_per"], init_hr_err_dict["mean_w_per"])
        pp_mean_err = get_mean_err(pp_blk_err_dict, pp_hr_err_dict["mean_r_per"], pp_hr_err_dict["mean_w_per"])
        diff_mean_err = init_mean_err - pp_mean_err

        final_dict = { 
            "mean": diff_mean_err,
            "rate": rate,
            "bits": bits,
            "seed": seed,
            "it": hr_err_iteration,
            "workload": workload_name
        }
        final_dict.update(blk_diff_dict)
        final_dict.update(hr_diff_dict)

        print(final_dict)
        final_perf_dict_arr.append(final_dict)

    return DataFrame(final_perf_dict_arr)