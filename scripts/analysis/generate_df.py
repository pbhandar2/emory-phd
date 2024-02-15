from pathlib import Path 
from argparse import ArgumentParser

OUTPUT_DIR = Path("/research2/mtc/cp_traces/pranav-phd/replay_output/set-0")


class OutputFile:
    def __init__(self, output_file_path):
        self._output_file_path = output_file_path 
    
    def get_stat_dict(self):
        cur_dict = {}
        with self._output_file_path.open("r") as f:
            line = f.readline().rstrip()
            while line:
                split_line = line.split("=")
                metric = split_line[0]
                metric_val = float(split_line[1])
                cur_dict[metric] = metric_val
                line = f.readline().rstrip()
        print(cur_dict)
        return cur_dict


def list_files(output_dir):
    for cur_path in output_dir.rglob("*"):
        if cur_path.stem == "stat_0":
            output_file_path = OutputFile(cur_path)
            cur_dict = output_file_path.get_stat_dict()
            print(cur_dict)
        print(cur_path)


def main():
    parser = ArgumentParser(description="Generate DF.")
    parser.add_argument("--machine", "-m", default="c220g5", type=str, help="Machine type.")
    args = parser.parse_args()
    list_files(OUTPUT_DIR.joinpath(args.machine))

if __name__ == "__main__":
    main()
        
    