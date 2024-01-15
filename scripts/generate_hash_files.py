from argparse import ArgumentParser

from keyuri.experiments.HashFileGeneration import HashFileGeneration

if __name__ == "__main__":
    parser = ArgumentParser(description="Compute cache feature from a cache trace.")
    parser.add_argument("workload_name", type=str, help="Name of workload.")
    parser.add_argument("seed", type=int, help="Random seed.")
    parser.add_argument("num_lower_addr_bits_ignored", type=int, help="Number of lower address bits ignored.")
    args = parser.parse_args()

    hash_file_generator = HashFileGeneration(args.workload_name, args.seed, args.num_lower_addr_bits_ignored)
    hash_file_generator.generate_hash_file()