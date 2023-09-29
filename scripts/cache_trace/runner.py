from pathlib import Path 
from argparse import ArgumentParser

from cydonia.profiler.CacheTrace import CacheTrace


def main():
    parser = ArgumentParser(description="Generate a cache trace in a remote node.")
    parser.add_argument("blk_trace_path", type=Path, help="Path to the block trace.")
    parser.add_argument("cache_trace_path", type=Path, help="Path to the block trace.")
    parser.add_argument("stack_binary_path", type=str, help="Path to binary that computes stack distance.")
    parser.add_argument("--trace_type", type=str, default="cache", help="Type of trace to generate cache or access.")
    args = parser.parse_args()

    cache_trace = CacheTrace(args.stack_binary_path)
    args.cache_trace_path.parent.mkdir(exist_ok=True, parents=True)

    if args.trace_type:
        cache_trace.generate_cache_trace(args.blk_trace_path, args.cache_trace_path)
        print("Generated cache trace {} for block trace {}".format(args.cache_trace_path, args.blk_trace_path))
    else:
        cache_trace.generate_access_trace(args.blk_trace_path, args.cache_trace_path)
        print("Generated access trace {} for block trace {}".format(args.cache_trace_path, args.blk_trace_path))


if __name__ == "__main__":
    main()