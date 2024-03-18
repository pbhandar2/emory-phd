from pathlib import Path 
from numpy import savetxt
from pandas import read_csv
from argparse import ArgumentParser

from PyMimircache.cacheReader.csvReader import CsvReader
from PyMimircache.profiler.cLRUProfiler import CLRUProfiler as LRUProfiler


class CreateRDTrace:
    def __init__(self, cache_trace_path: Path):
        self._cache_trace_path = cache_trace_path
    

    def create(self, rd_trace_path: Path):
        init_params = { "label": 3 }
        csv_reader = CsvReader(str(self._cache_trace_path), init_params=init_params)
        rd_arr = LRUProfiler(csv_reader).get_reuse_distance().astype(int)
        savetxt(str(rd_trace_path), rd_arr, fmt='%i', delimiter='\n')

        print("RD trace {} created.".format(rd_trace_path))
        rd_trace_df = read_csv(rd_trace_path, names=['rd'])
        cache_trace_df = read_csv(self._cache_trace_path, names=["i", "iat", "key", "op", "front_misalign", "rear_misalign"])
        assert len(rd_trace_df) == len(cache_trace_df), \
            "Rd trace len {} and cache trace len {} not equal".format(len(rd_trace_df), len(cache_trace_df))


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