from argparse import ArgumentParser
from pandas import read_csv 

from keyuri.experiments.PPTargetRate import PPTargetRate


if __name__ == "__main__":
    parser = ArgumentParser(description="Post process samples.")

    parser.add_argument("--workload",
                            "-w", 
                            type=str, 
                            help="Name of workload.")

    parser.add_argument("--type",
                            "-t", 
                            type=str, 
                            default="basic",
                            help="The sample type.")
    
    parser.add_argument("--metric",
                            "-m", 
                            type=str, 
                            help="The metric used by the post-processing algorithm.")
    
    parser.add_argument("--rate",
                            "-r",
                            type=float, 
                            help="Sampling rate in percentage.")

    parser.add_argument("--seed",
                            "-s",
                            type=int, 
                            help="Random seed of the sample.")

    parser.add_argument("--bits",
                            "-b",
                            type=int, 
                            help="Number of lower order bits of addresses ignored.")
    
    parser.add_argument("--abits",
                            "-a",
                            type=int, 
                            help="Number of lower order bits of addresses ignored.")

    parser.add_argument("--targetrate",
                            "-tr",
                            type=float, 
                            help="Target sampling rate.")
    
    args = parser.parse_args()
    print("Post processing samples with args: {}".format(args))

    cp_feature_df = read_csv("./test/block.csv")
    block_count = int(cp_feature_df[cp_feature_df["workload"]==args.workload]["wss"]/4096)

    post_process = PPTargetRate(args.workload,
                                        args.type,
                                        args.metric,
                                        args.rate,
                                        args.bits,
                                        args.seed,
                                        args.abits,
                                        block_count)
    
    post_process.run_best(args.targetrate)
    
    