from pathlib import Path
from argparse import ArgumentParser 
from itertools import product 

from keyuri.config.BaseConfig import BaseConfig
from cydonia.blksample.sample import get_sampled_blocks, generate_sample_cache_trace


class SampleCacheTrace:
    def __init__(self, cache_trace_path: Path):
        self._cache_trace_path = cache_trace_path
    

    def create(self, 
                sample_cache_trace_path: Path,
                rate: float, 
                seed: int,
                bits: int 
    ) -> None:
        sample_blk_dict = get_sampled_blocks(self._cache_trace_path, rate, seed, bits)
        generate_sample_cache_trace(self._cache_trace_path, sample_blk_dict, sample_cache_trace_path)


def main():
    parser = ArgumentParser(description="Post process sample block traces.")

    parser.add_argument("-w",
                            "--workloads",
                            nargs='+',
                            type=str,
                            help="Workloads to sample.",
                            required=True)

    parser.add_argument("-t",
                            "--types",
                            nargs='+',
                            type=str,
                            help="Sample types to generate.",
                            required=True)

    parser.add_argument("-r",
                            "--rates", 
                            nargs='+', 
                            type=int, 
                            help="Sampling rate in percentage.", 
                            required=True)

    parser.add_argument("-b",
                            "--bits", 
                            nargs='+', 
                            type=int, 
                            help="Number of lower order bits of addresses ignored.", 
                            required=True)
    
    parser.add_argument("-s",
                            "--seeds", 
                            nargs='+', 
                            type=int, 
                            help="Random seed of the sample.", 
                            required=True)
    
    args = parser.parse_args()

    config = BaseConfig()
    for cache_trace_path in config.get_all_cache_traces():
        for sample_type, rate, bits, seed in product(args.types, args.rates, args.bits, args.seeds):
            workload_name = cache_trace_path.stem
            if workload_name not in args.workloads:
                continue 

            sample_cache_trace_path = config.get_sample_cache_trace_path(sample_type, workload_name, rate, bits, seed)
            if sample_cache_trace_path.exists():
                print("Sample cache trace path {} already exists!".format(cache_trace_path))
                #continue 

            sample_cache_trace_path.parent.mkdir(exist_ok=True, parents=True)
            print("Generating sample cache trace {}.".format(sample_cache_trace_path))
            cache_trace = SampleCacheTrace(cache_trace_path)
            cache_trace.create(sample_cache_trace_path, float(rate)/100.0, seed, bits)


if __name__ == "__main__":
    main()