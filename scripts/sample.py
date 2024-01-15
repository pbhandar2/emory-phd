from argparse import ArgumentParser

from keyuri.experiments.SampleCacheTrace import SampleCacheTrace


if __name__ == "__main__":
    parser = ArgumentParser(description="Sample cache traces.")

    parser.add_argument("workload_name", 
                            type=str, 
                            help="Name of workload.")

    parser.add_argument("sample_set_name", 
                            type=str, 
                            help="Name of sample set.")

    parser.add_argument("-r",
                            "--rate", 
                            nargs='+', 
                            type=float, 
                            help="Sampling rate in percentage.", 
                            required=True)

    parser.add_argument("-s",
                            "--seed", 
                            nargs='+', 
                            type=int, 
                            help="Random seed of the sample.", 
                            required=True)

    parser.add_argument("-b",
                            "--bits", 
                            nargs='+', 
                            type=int, 
                            help="Number of lower order bits of addresses ignored.", 
                            required=True)

    args = parser.parse_args()

    sample = SampleCacheTrace(args.workload_name, args.sample_set_name)
    sample.sample(args.rate, args.bits, args.seed)