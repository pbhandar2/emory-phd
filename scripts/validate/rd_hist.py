from pandas import read_csv 
from numpy import zeros
from scipy.stats import describe

if __name__ == "__main__":

    workload = "w54"
    rate = 10
    bits = 4
    seed = 42

    full_cache_trace_path = "/research2/mtc/cp_traces/pranav-phd/cp/cache_traces/{}.csv".format(workload)
    full_rd_trace_path = "/research2/mtc/cp_traces/pranav-phd/cp/rd_traces/{}.csv".format(workload)
    full_rd_hist_path = "/research2/mtc/cp_traces/pranav-phd/cp/rd_hists/{}.csv".format(workload)

    sample_cache_trace_path = "/research2/mtc/cp_traces/pranav-phd/cp--basic/cache_traces/{}/{}_{}_{}.csv".format(workload, rate, bits, seed)
    sample_rd_trace_path = "/research2/mtc/cp_traces/pranav-phd/cp--basic/rd_traces/{}/{}_{}_{}.csv".format(workload, rate, bits, seed)
    sample_rd_hist_path = "/research2/mtc/cp_traces/pranav-phd/cp--basic/rd_hists/{}/{}_{}_{}.csv".format(workload, rate, bits, seed)

    full_rd_trace = read_csv(full_rd_trace_path, names=["rd"])
    full_cache_trace_df =  read_csv(full_cache_trace_path, names=["i", "ts", "lba", "op", "f", "r"])
    full_cache_trace_df["rd"] = full_rd_trace["rd"]

    #print(full_cache_trace_df)

    sample_rd_trace = read_csv(sample_rd_trace_path, names=["rd"])
    sample_cache_trace_df =  read_csv(sample_cache_trace_path, names=["i", "ts", "lba", "op", "f", "r"])
    sample_cache_trace_df["rd"] = sample_rd_trace["rd"]

    #print(sample_cache_trace_df)

    full_r_df = full_cache_trace_df[(full_cache_trace_df["rd"] == -1) & (full_cache_trace_df["op"]=="r")]
    full_w_df = full_cache_trace_df[(full_cache_trace_df["rd"] == -1) & (full_cache_trace_df["op"]=="w")]

    sample_r_df = sample_cache_trace_df[(sample_cache_trace_df["rd"] == -1) & (sample_cache_trace_df["op"]=="r")]
    sample_w_df = sample_cache_trace_df[(sample_cache_trace_df["rd"] == -1) & (sample_cache_trace_df["op"]=="w")]

    print(len(full_r_df)/1e6, len(full_w_df)/1e6, len(sample_r_df)/1e6, len(sample_w_df)/1e6)

    full_rd_hist = read_csv(full_rd_hist_path, names=["r", "w"])
    sample_rd_hist = read_csv(sample_rd_hist_path, names=["r", "w"])

    print(len(full_rd_hist)/(256 * 1024), len(sample_rd_hist)/(256*1024))

    full_cum_rd_hist = full_rd_hist[1:].cumsum()
    total_full_req_count = full_rd_hist.sum().sum()

    sample_cum_rd_hist = sample_rd_hist[1:].cumsum()
    total_sample_req_count = sample_rd_hist.sum().sum()

    sample_hr_arr = zeros(len(sample_cum_rd_hist))
    for row_i, row in sample_cum_rd_hist.iterrows():
        sample_hit_rate = row.sum()/total_sample_req_count
        sample_hr_arr[int(row_i) - 1] = sample_hit_rate
    
    print(sample_hr_arr[:10], sample_hr_arr[-10:])
    print(len(sample_hr_arr)/(256 * 1024))

    full_hr_arr = zeros(len(full_cum_rd_hist))
    for row_i, row in full_cum_rd_hist.iterrows():
        full_hit_rate = row.sum()/total_full_req_count
        full_hr_arr[int(row_i) - 1] = full_hit_rate
    
    print(full_hr_arr[:10], full_hr_arr[-10:])
    print(len(full_hr_arr)/(256 * 1024))

    hr_err_arr = []
    for i in range(len(sample_hr_arr)):
        sample_hr = sample_hr_arr[i]
        full_size = (i+1)*rate 

        if full_size > len(full_hr_arr):
            break 
        else:
            err = abs(full_hr_arr[full_size] - sample_hr)
            hr_err_arr.append(err)
    
    print(describe(hr_err_arr))