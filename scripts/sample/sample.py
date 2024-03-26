from argparse import ArgumentParser
from pathlib import Path 

from cydonia.profiler.CacheTrace import CacheTraceReader


def main():
    parser = ArgumentParser(description="Sample a cache trace using a hash file.")
    parser.add_argument("-c", "--cache_trace", type=Path, help="Path to cache trace.") 
    parser.add_argument("-hf", "--hash_file", type=Path, help="Path to hash file.") 
    parser.add_argument("-s", "--sample_file", type=Path, help="Path to sample file.")
    parser.add_argument("-r", "--rate", type=float, help="Sampling ratio.")
    parser.add_argument("-b", "--bits", type=int, help="The number of lower order address bits ignored.")
    args = parser.parse_args()

    args.sample_file.parent.mkdir(exist_ok=True, parents=True)
    reader = CacheTraceReader(args.cache_trace)
    reader.sample_using_hash_file(args.hash_file, args.rate, args.bits, args.sample_file)


if __name__ == "__main__":
    main()