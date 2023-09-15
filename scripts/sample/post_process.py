"""This scripts runs post-processing on samples. 

Usage:
    python3 post_process.py workload_name rate seed bits 
"""

from argparse import ArgumentParser


def main():
    parser = ArgumentParser(description="Post process sample block traces.")
    parser.add_argument("workload_name", type=str, help="The name of the workload.")
    parser.add_argument("rate", type=int, help="The rate of sampling.")
    parser.add_argument("seed", type=int, help="Random seed.")
    parser.add_argument("bits", type=int, help="Number of lower order bits of addresses that are ignored.")
    parser.add_argument("max_effective_rate", type=int, help="Maximum effective sample rate after post processing.")
    parser.add_argument("--sample_type", type=str, default="iat", help="The type of sampling technique used.")
    parser.add_argument("--workload_type", type=str, default="cp", help="The type of workload.")
    parser.add_argument("--priority", type=str, default="err", help="The priority metric.")
    args = parser.parse_args()




if __name__ == "__main__":
    main()
    