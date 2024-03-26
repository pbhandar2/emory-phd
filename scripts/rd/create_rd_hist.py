from pathlib import Path 
from argparse import ArgumentParser

from rd_trace import create_rd_hist


def main():
    parser = ArgumentParser(description="Compute RD trace.")
    parser.add_argument("cache_trace_file", type=Path, help="Cache trace file.")
    parser.add_argument("rd_trace_file", type=Path, help="RD trace file.")
    parser.add_argument("rd_hist_file", type=Path, help="The RD hist file.")
    args = parser.parse_args()

    create_rd_hist(args.cache_trace_file, args.rd_trace_file, args.rd_hist_file)


if __name__ == "__main__":
    main()