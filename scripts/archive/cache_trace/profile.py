from argparse import ArgumentParser
from keyuri.analysis.ProfileCacheTrace import ProfileCacheTrace


def main():
    parser = ArgumentParser(description="Generate a cache trace in a remote node.")
    parser.add_argument("workload_name", type=str, help="Name of the workload")
    parser.add_argument("rate", type=int, help="Sampling rate.")
    parser.add_argument("bits", type=int, help="Number of lower-order bits ignored.")
    parser.add_argument("seed", type=int, help="Random seed.")
    parser.add_argument("sample_type", type=str, help="The sample type.")
    parser.add_argument("--force_flag", action='store_true', help="If true, re-computes cache features even if output file exists.")
    args = parser.parse_args()

    print("Profiling cache trace for workload {} with rate {}, bits {}, seed {} for sample type {} using force flag {}.".format(args.workload_name,
                                                                                                                                    args.rate,
                                                                                                                                    args.bits,
                                                                                                                                    args.seed,
                                                                                                                                    args.sample_type,
                                                                                                                                    args.force_flag))

    profiler = ProfileCacheTrace(args.workload_name, sample_type=args.sample_type)
    profiler.profile(args.rate, args.bits, args.seed, force_flag=args.force_flag)


if __name__ == "__main__":
    main()