from argparse import ArgumentParser
from pathlib import Path 
from json import dump 

from keyuri.analysis.CompileHrErr import CompileHrErr

def main():
    parser = ArgumentParser(description="Compute HR error of samples.")
    parser.add_argument("full_rd_hist_file", type=Path, help="Path to RD histogram of full workload.")
    parser.add_argument("sample_rd_hist_file", type=Path, help="Path to RD histgoram of sample workload.")
    parser.add_argument("output_dir", type=Path, help="The directory to output hit rate error file.")
    parser.add_argument("sampling_ratio", type=float, help="The sampling ratio used to generate the sample.")
    args = parser.parse_args()

    compile_hr_err = CompileHrErr()
    err_dict = compile_hr_err.compute_sample_hr_err(args.full_rd_hist_file, args.sample_rd_hist_file, args.sampling_ratio)
    output_path = args.output_dir.joinpath(args.sample_rd_hist_file.name)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w+") as output_handle:
        dump(err_dict, output_handle, indent=2)






if __name__ == "__main__":
    main()