from argparse import ArgumentParser

from keyuri.experiments.PostProcessSample import PostProcessSample


def main():
    parser = ArgumentParser(description="Post process sample block traces using brute force.")
    parser.add_argument("workload_name", type=str, help="The name of the workload.")
    parser.add_argument("rate", type=int, help="The rate of sampling.")
    parser.add_argument("seed", type=int, help="Random seed.")
    parser.add_argument("bits", type=int, help="Number of lower order bits of addresses that are ignored in the sample.")
    parser.add_argument("algo_bits", type=int, help="Number of lower order bits of addresses that are ignored by the algorithm.")
    parser.add_argument("--sample_type", type=str, default="iat", help="The type of sampling technique used.")
    parser.add_argument("--workload_type", type=str, default="cp", help="The type of workload.")
    parser.add_argument("--break_flag", action='store_true', help="If set to true, remove the first block you find that reduces error (default). Else, evaluate all blocks.")
    args = parser.parse_args()
    
    if args.break_flag:
        algo_name = "first"
    else:
        algo_name = "best"

    post_processor = PostProcessSample(args.workload_name)
    post_processor.post_process(args.rate, args.bits, args.seed, args.algo_bits, sample_type=args.sample_type, algo_type=algo_name)


if __name__ == "__main__":
    main()