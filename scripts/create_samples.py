from argparse import ArgumentParser

from keyuri.experiments.CreateSamples import CreateSamples


if __name__ == "__main__":
    parser = ArgumentParser(description="Create sample block traces.")

    parser.add_argument("workload_name", type=str, help="Name of the workload.")

    parser.add_argument("--workload_type", default="cp", type=str, help="Workload type.")

    args = parser.parse_args()

    create_samples = CreateSamples()
    create_samples.create(args.workload_name, workload_type=args.workload_type)