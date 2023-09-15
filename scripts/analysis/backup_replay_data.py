"""This script backups the output of trace replay if necessary. 

Usage:
    python3 backup_replay_data.py --replay_output_dir_path /path/to/replay --replay_output_backup_dir /path/to/backup
"""
from pathlib import Path 
from argparse import ArgumentParser 

from keyuri.analysis.ReplayDB import ReplayDB


def backup(
        main_dir: Path, 
        backup_dir: Path
) -> None:
    db = ReplayDB(main_dir, backup_dir)
    latest_backup_dir_name = db.get_current_backup()
    if len(latest_backup_dir_name):
        print("Backup not necessary. Updated backup in {}".format(backup_dir.joinpath(latest_backup_dir_name)))
    else:
        print("Backing up replay data.")
        db.backup()
        print("Done.")


def main():
    parser = ArgumentParser(description="Backup replay data if necessary.")
    parser.add_argument("--replay_output_dir", 
                        type=Path, 
                        default=Path("/research2/mtc/cp_traces/pranav/replay"),
                        help="Path of directory containing replay output file.")
    parser.add_argument("--replay_output_backup_dir", 
                        type=Path, 
                        default=Path("/research2/mtc/cp_traces/pranav/replay_backup"),
                        help="Path of directory containing replay output file.")
    args = parser.parse_args()
    backup(args.replay_output_dir, args.replay_output_backup_dir)


if __name__ == "__main__":
    main()