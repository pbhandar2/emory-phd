from argparse import ArgumentParser
from pathlib import Path

from keyuri.config.BaseConfig import BaseConfig
from cydonia.profiler.CPReader import CPReader


class CreateCacheTrace:
    def __init__(self, block_trace_path: Path):
        self._block_trace_path = block_trace_path
        self._reader = CPReader(str(block_trace_path))
    

    def create(self, cache_trace_path: Path):
        self._reader.generate_cache_trace(cache_trace_path)


def main():
    parser = ArgumentParser(description="Create cache trace from block traces.")

    parser.add_argument("-w",
                            "--workloads",
                            nargs='+',
                            type=str,
                            help="Workloads to generate cache traces for.",
                            required=True)

    args = parser.parse_args()

    config = BaseConfig()
    for block_trace_path in config.get_all_block_traces():
        workload_name = block_trace_path.stem 
        if workload_name not in args.workloads:
            continue 
        
        cache_trace_path = config.get_cache_trace_path(block_trace_path.stem)
        if cache_trace_path.exists():
            print("Cache trace path {} already exists!".format(cache_trace_path))

        print("Generating cache trace {}.".format(cache_trace_path))
        cache_trace = CreateCacheTrace(block_trace_path)
        cache_trace.create(cache_trace_path)


if __name__ == "__main__":
    main()
