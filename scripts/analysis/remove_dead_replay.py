"""This scripts removes directory of all dead experiments.

Usage:
    python3 remove_dead_replay.py --node_config_file_path /path/to/node/config.json --replay_output_dir /path/to/replay
"""

from json import load 
from argparse import ArgumentParser
from pathlib import Path 

from keyuri.analysis.ReplayDB import ReplayDB


def remove_dead_replay(
        node_config_file_path: Path, 
        replay_output_dir: Path,
        replay_output_backup_dir: Path 
) -> None:
    with node_config_file_path.open("r") as node_config_file_handle:
        node_config = load(node_config_file_handle)
    
    node_dict = node_config["nodes"]
    hostname_list = [node_dict[key]["host"] for key in node_dict]

    db = ReplayDB(replay_output_dir, replay_output_backup_dir)
    removed_experiment_arr = db.delete_dead_experiments(hostname_list)

    print("Removed {} dead experiments.".format(len(removed_experiment_arr)))
    for removed_experiment_dir in removed_experiment_arr:
        print("Removed file {}.".format(removed_experiment_dir))


def main():
    parser = ArgumentParser(description="Remove directory of dead replay.")
    parser.add_argument("--node_config_file_path",
                        type=Path,
                        default=Path("../runner/config.json"),
                        help="Path of configuration file containing all the live nodes.")
    parser.add_argument("--replay_output_dir", 
                        type=Path, 
                        default=Path("/research2/mtc/cp_traces/pranav/replay"),
                        help="Path of directory containing replay output file.")
    parser.add_argument("--replay_output_backup_dir", 
                        type=Path, 
                        default=Path("/research2/mtc/cp_traces/pranav/replay_backup"),
                        help="Path of directory containing replay output file.")
    args = parser.parse_args()
    remove_dead_replay(args.node_config_file_path, args.replay_output_dir, args.replay_output_backup_dir)


if __name__ == "__main__":
    main()