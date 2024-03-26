from pathlib import Path 
from argparse import ArgumentParser

from rd_trace import CreateRDTrace


def main():
    parser = ArgumentParser(description="Compute RD trace.")
    parser.add_argument("cache_trace_file", type=Path, help="Cache trace file.")
    parser.add_argument("rd_trace_file", type=Path, help="RD trace file.")
    args = parser.parse_args()

    create_rd_trace = CreateRDTrace(args.cache_trace_file)
    args.rd_trace_file.parent.mkdir(exist_ok=True, parents=True)
    create_rd_trace.create(args.rd_trace_file)


if __name__ == "__main__":
    main()