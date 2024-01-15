from argparse import ArgumentParser 

from keyuri.config.BaseConfig import BaseConfig
from cydonia.profiler.CacheTraceProfiler import generate_block_trace, validate_cache_trace


def main():
    parser = ArgumentParser(description="Create block trace from cache trace.")
    parser.add_argument("-t",
                            "--type",
                            type=str,
                            default="iat",
                            help="The sample type.")
    parser.add_argument("-w",
                            "--workload",
                            type=str,
                            help="Name of the workload.",
                            required=True)
    parser.add_argument("-r",
                            "--rate",
                            type=int,
                            help="The sampling rate.",
                            required=True)
    parser.add_argument("-b",
                            "--bits",
                            type=int,
                            help="Number of lower order bits ignored during sampling.",
                            required=True)
    parser.add_argument("-s",
                            "--seed",
                            type=int,
                            help="Random seed used during sampling.",
                            required=True)
    parser.add_argument("-v",
                            action="store_true",
                            help="Whether to validate the block trace created.")
    args = parser.parse_args()

    config = BaseConfig()
    sample_cache_trace_path_list = config.get_all_sample_cache_traces(args.type, args.workload)
    for cache_trace_path in sample_cache_trace_path_list:
        if ".rd" in cache_trace_path.name:
            continue 
        split_cache_file_name = cache_trace_path.stem.split('_')
        rate, bits, seed = int(split_cache_file_name[0]), int(split_cache_file_name[1]), int(split_cache_file_name[2])

        if rate != args.rate:
            continue 

        if bits != args.bits:
            continue 

        if seed != args.seed:
            continue 

        block_trace_path = config.get_sample_block_trace_path(args.type, args.workload, rate, bits, seed)
        if not block_trace_path.exists():
            print("Generating block trace {} from cache trace {}.".format(block_trace_path, cache_trace_path))
            block_trace_path.parent.mkdir(exist_ok=True, parents=True)
            generate_block_trace(cache_trace_path, block_trace_path)
        else:
            print("Already exists block trace {} from cache trace {}.".format(block_trace_path, cache_trace_path))
        
        if args.v:
            validate_cache_trace(block_trace_path, cache_trace_path)


if __name__ == "__main__":
    main()