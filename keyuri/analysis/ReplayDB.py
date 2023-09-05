"""ReplayDB loads the data from output files from block trace replay to run analysis
and present it to the user. 

Usage:
    db = ReplayDB("/path/to/replay/output/")

    # remove live experiments whose nodes are no longer up 
    db.delete_dead_experiments(list_of_live_host_names)
"""

from filecmp import cmp 
from time import strftime
from shutil import copy 
from pathlib import Path 
from collections import defaultdict


class ReplayDB:
    def __init__(
            self,
            replay_output_dir: Path,
            backup_dir: Path 
    ) -> None:
        """Create a DB of data in replay output files. 

        Args:
            replay_output_dir: Path of directory containing replay output. 
            backup_dir: Path of directory to backup files in the replay output directory. 

        Attributes:
            _output_dir: Path of directory containing replay output. 
            _backup_dir: Path of directory to backup files in the replay output directory. 
            _live_experiment_arr: Array of paths to directory of live experiments. 
            _complete_experiment_arr: Array of paths to directory of completed experiments. 
        """
        self._output_dir = replay_output_dir
        self._backup_dir = backup_dir


    def backup(self) -> None:
        """Backup the files from the replay output directory."""
        backup_dir_name = strftime("%Y%m%d-%H%M%S")
        backup_dir = self._backup_dir.joinpath(backup_dir_name)
        backup_dir.mkdir(exist_ok=True)
        self.copy_dir(self._output_dir, backup_dir)
    

    def get_current_backup(self) -> str:
        """Get the name of the backup that matches the current state of replay output directory. 

        Returns:
            latest_backup_name: Name of the backup matching the current state of replay output directory. 
        """
        latest_backup_name = '' 
        for backup_dir in self._backup_dir.iterdir():
            if self.compare_dir(self._output_dir, backup_dir):
                latest_backup_name = backup_dir.name
                break 
        return latest_backup_name
    

    def delete_dead_experiments(
            self, 
            live_host_name_list: list 
    ) -> list:
        """Delete experiments that were started but the nodes where they 
        were started are no longer running.

        Args:
            live_host_name_list: List of host names that are live. If the host name of the experiment is not
                                    in the list, it is deleted. 
        
        Returns:
            dead_experiment_list: List of path of experiment directories that were deleted. 
        """
        live_experiment_arr, _ = self._get_live_and_complete_experiment()
        for live_experiment_dir in live_experiment_arr:
            with live_experiment_dir.joinpath("host").open("r") as host_file_handle:
                host_name = host_file_handle.read().rstrip()
            if host_name not in live_host_name_list:
                self.rm_dir(live_experiment_dir)
    

    def _get_live_and_complete_experiment(self) -> tuple:
        """Load the experiment output files."""
        live_experiment_arr, complete_experiment_arr = [], []
        for cur_path in self._output_dir.glob('**/host'):
            if cur_path.is_dir() or cur_path.stem != "host":
                continue 
            
            # A directory of a live experiment will have a single file "host" whereas 
            # a directory of a completed experiment will have more than 1 files. 
            replay_output_dir = cur_path.parent 
            file_count = len(list(replay_output_dir.iterdir()))
            if file_count == 1:
                live_experiment_arr.append(replay_output_dir)
            elif file_count > 1:
                complete_experiment_arr.append(replay_output_dir)
            else:
                raise ValueError("Check {}. Directory contains no file. ReplayDB data corrupt.".format(replay_output_dir))
        return (live_experiment_arr, complete_experiment_arr)
    

    @staticmethod
    def copy_dir(
        source_dir: Path,
        target_dir: Path 
    ) -> None:
        """Create a copy of all files in the source directory in the target directory. 

        Args:
            source_dir: Path of the source directory. 
            target_dir: Path of the target directory. 
        
        Raises:
            ValueError: Raised if the target directory is not empty. 
        """
        target_dir_str = str(target_dir.expanduser())
        source_dir_str = str(source_dir.expanduser())
        for cur_path in source_dir.glob('**/*'):
            remapped_path = Path(str(cur_path.expanduser()).replace(source_dir_str, target_dir_str))
            if cur_path.is_file():
                remapped_path.parent.mkdir(exist_ok=True, parents=True)
                if remapped_path.exists() and cmp(cur_path, remapped_path, shallow=False):
                        continue 
                copy(str(cur_path.expanduser()), str(remapped_path.expanduser()))
            else:
                file_count = 0 
                for _ in cur_path.iterdir():
                    file_count += 1
                if file_count == 0:
                    remapped_path.mkdir(exist_ok=True, parents=True)
    

    @staticmethod
    def rm_dir(dir: Path):
        """Remove all files in the directory and the directory.
        
        Args:
            dir: Path of directory to be removed. 
        """
        dir_dict = defaultdict(list)
        for cur_path in dir.glob('**/*'):
            # delete the files first to empty all directories 
            # while tracking the directory and their depth to delete them 
            # once we know its empty 
            if cur_path.is_file():
                cur_path.unlink()
            else:
                depth = 0 
                temp_path = cur_path 
                while temp_path != dir:
                    depth += 1 
                    temp_path = temp_path.parent 
                dir_dict[depth].append(cur_path)
        
        # delete directory with the highest depth first to make sure 
        # all directories are empty when we attempt to delete them 
        sorted_depth_val_list = sorted(dir_dict.keys(), reverse=True)
        for depth_val in sorted_depth_val_list:
            for sub_dir_path in dir_dict[depth_val]:
                sub_dir_path.rmdir()

        dir.rmdir()
    

    @staticmethod
    def compare_dir(
        source_dir: Path,
        target_dir: Path 
    ) -> bool:
        """Create a copy of all files in the source directory in the target directory. 

        Args:
            source_dir: Path of the source directory. 
            target_dir: Path of the target directory. 
        
        Returns:
            is_same: Boolean indicating if the two directories are the same. 
        """
        is_same = False 
        target_dir_str = str(target_dir.expanduser())
        source_dir_str = str(source_dir.expanduser())
        for cur_path in source_dir.glob('**/*'):
            remapped_path = Path(str(cur_path.expanduser()).replace(source_dir_str, target_dir_str))

            print(remapped_path, remapped_path.exists())
            if not remapped_path.exists():
                break 

            if remapped_path.is_dir():
                continue 

            print(cur_path, remapped_path, cmp(cur_path, remapped_path, shallow=False))
            if not cmp(cur_path, remapped_path, shallow=False):
                break 
        else:
            is_same = True 
        
        return is_same


    @property
    def live_experiment_arr(self):
        live_experiment_arr, _ = self._get_live_and_complete_experiment()
        return live_experiment_arr


    @property
    def complete_experiment_arr(self):
        _, complete_experiment_arr = self._get_live_and_complete_experiment()
        return complete_experiment_arr