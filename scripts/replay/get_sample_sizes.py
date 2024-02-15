from argparse import ArgumentParser
from pathlib import Path 


def get_tier_size_list(cache_trace_path: Path):
    unique_blk_set = set()
    with cache_trace_path.open("r") as trace_handle:
        line = trace_handle.readline().rstrip()
        while line:
            unique_blk_set.add(int(line.split(",")[2]))
            line = trace_handle.readline().rstrip()
    
    num_blocks = len(unique_blk_set)
    print("WSS has {} blocks = {} MB".format(num_blocks, num_blocks/(256)))

    max_cache_size = 100 * (((num_blocks/256)//100) + 1)
    print("Max cache size {}".format(max_cache_size))

    scaler_list = [0.25, 0.5, 0.75, 1.0]
    min_t1_size = 100
    min_t2_size = 150 
    max_t1_size = 150 * 1000
    max_t2_size = 400 * 1000

    if max_cache_size < 200:
        print("Max cache size too low {}".format(max_cache_size))
        return 

    size_list = []
    for t1_scaler in scaler_list:
        t1_size = max_cache_size * t1_scaler
        if t1_size > min_t1_size and t1_size <= max_t1_size:
            size_list.append([t1_size, 0])
        for t2_scaler in scaler_list:
            t2_size = max_cache_size * t2_scaler
            if t1_size > min_t1_size and t1_size <= max_t1_size and t2_size > min_t2_size and t2_size <= max_t2_size:
                size_list.append([t1_size, t2_size])

    return num_blocks, size_list 


def main():
    parser = ArgumentParser(description="Get tier sizes to be replayed given a cache trace.")
    parser.add_argument("cache_trace_path", type=Path, help="Path of cache trace.")
    args = parser.parse_args()

    if not args.cache_trace_path.exists():
        print("{} does not exist.".format(args.cache_trace_path))
        return 

    tier_size_list = get_tier_size_list(args.cache_trace_path)


if __name__ == "__main__":
    main()