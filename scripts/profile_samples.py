from argparse import ArgumentParser

from keyuri.experiments.ProfileSamples import ProfileSamples


if __name__ == "__main__":
    parser = ArgumentParser(description="Create sample block traces.")

    parser.add_argument("workload_name", type=str, help="Name of the workload.")

    parser.add_argument("--workload_type", default="cp", type=str, help="Workload type.")

    args = parser.parse_args()

    profile_samples = ProfileSamples()
    profile_samples.profile(args.workload_name, workload_type=args.workload_type)