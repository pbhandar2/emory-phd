from pandas import read_csv 
from argparse import ArgumentParser

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

    parser.add_argument("--pmetric",
                            "-pm",
                            type=str,
                            help="The priority metric to select blocks to evaluate.")
    
    parser.add_argument("--neval",
                            "-n",
                            type=int,
                            help="The number of improving evaluation to consider before terminating.")
    
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

    metric_name = "first_{}_{}_{}".format(args.neval, args.metric, args.pmetric)

    post_process = PPTargetRate(args.workload,
                                        args.type,
                                        metric_name,
                                        args.rate,
                                        args.bits,
                                        args.seed,
                                        args.abits,
                                        block_count)
    
    post_process.run(args.targetrate)
    
    