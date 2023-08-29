from argparse import ArgumentParser

from keyuri.experiments.CreateCacheTrace import CreateCacheTrace


if __name__ == "__main__":
    parser = ArgumentParser(description="Create cache trace from block trace. ")

    parser.add_argument("workload_name", type=str, help="Name of the workload.")

    parser.add_argument("stack_binary_path", type=str, help="Path to binary that computes stack distance.")

    parser.add_argument("--workload_type", default="cp", type=str, help="Workload type.")

    args = parser.parse_args()

    create_samples = CreateCacheTrace(args.stack_binary_path)
    create_samples.create(args.workload_name, workload_type=args.workload_type)