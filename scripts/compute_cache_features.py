from argparse import ArgumentParser

from keyuri.experiments.CacheFeatures import CacheFeatures

if __name__ == "__main__":
    parser = ArgumentParser(description="Compute cache feature from a cache trace.")
    parser.add_argument("workload_name", type=str, help="Name of workload.")
    parser.add_argument("-t",
                            "--type",
                            type=str,
                            help="The type of sample.")
    parser.add_argument("-r",
                            "--rate",
                            type=float,
                            help="Sampling Rate.")
    
    parser.add_argument("-b",
                            "--bits",
                            type=int,
                            help="Number of lower order bits of block keys ignored during sampling.")

    parser.add_argument("-s",
                            "--seed",
                            type=int,
                            help="Random seed.")
    args = parser.parse_args()
    print(args)

    if not args.rate and not args.bits and not args.seed and not args.type:
        print("Full trace {} features computing...".format(args.workload_name))
        cache_features = CacheFeatures(args.workload_name)
        cache_features.generate_cache_feature_files()
    else:
        print("Sample trace of workload {} and type {} with seed {}, bits {} and rate {} features computing..".format(args.workload_name,
                                                                                                                        args.type,
                                                                                                                        args.seed,
                                                                                                                        args.bits,
                                                                                                                        args.rate))
        cache_features = CacheFeatures(args.workload_name)
        cache_features.generate_sample_cache_feature_files(args.type, args.bits, args.seed, args.rate)