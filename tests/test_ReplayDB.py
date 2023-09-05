from pathlib import Path 
from unittest import main, TestCase
from pathlib import Path 

from keyuri.analysis.ReplayDB import ReplayDB


def get_test_replay_dir_path() -> Path:
    pth = Path("../data/replay_db_test")
    if pth.exists():
        ReplayDB.rm_dir(pth)
    pth.mkdir(exist_ok=True)
    return pth 


def get_backup_test_replay_dir_path() -> Path:
    pth = Path("../data/backup_replay_db_test")
    if pth.exists():
        ReplayDB.rm_dir(pth)
    pth.mkdir(exist_ok=True)
    return pth 


def get_test_experiment_arr() -> list:
    experiment1_name = "q=128_bt=16_at=16_t1=100_t2=0_rr=1_it=0"
    experiment2_name = "q=128_bt=16_at=16_t1=150_t2=0_rr=1_it=0"
    experiment3_name = "q=128_bt=16_at=16_t1=200_t2=0_rr=1_it=0"
    return [experiment1_name, experiment2_name, experiment3_name]


def get_experiment_output_file_name_arr() -> list:
    return ["tsstat_0.out", "stat_0.out", "host", "usage.csv", "power.csv", "stdout.dump", "config.json"]


def create_host_file(experiment_output_dir_path: Path) -> None:
    experiment_output_dir_path.mkdir(exist_ok=True)
    host_file_path = experiment_output_dir_path.joinpath("host")
    with host_file_path.open("w+") as write_handle:
        write_handle.write("test_host_name")


def make_experiment_complete(experiment_output_dir_path: Path) -> None:
    experiment_output_file_arr = get_experiment_output_file_name_arr()
    for experiment_output_file_name in experiment_output_file_arr:
        experiment_output_file_path = experiment_output_dir_path.joinpath(experiment_output_file_name)
        with experiment_output_file_path.open("w+") as write_handle:
            write_handle.write("test_content")


def setup_test_replay_db_dir(test_dir_path: Path) -> None:
    """Setup the test directory with the necessary files and folders."""
    # create a directory for machine type 
    machine_type_data_dir = test_dir_path.joinpath("test_machine")
    machine_type_data_dir.mkdir(exist_ok=True)

    # create a directory for workload type and workload name 
    workload_dir = machine_type_data_dir.joinpath("test_workload_type", "test_workload_name")
    workload_dir.mkdir(exist_ok=True, parents=True)

    # create directories for different experiments 
    experiment_arr = get_test_experiment_arr()
    for experiment_name in experiment_arr:
        experiment_output_dir_path = workload_dir.joinpath(experiment_name)
        create_host_file(experiment_output_dir_path)
    
    # setup experiment 1 to have the complete set of output files 
    make_experiment_complete(workload_dir.joinpath(experiment_arr[0]))

    
class TestReplayDB(TestCase):
    def test_basic(self):
        replay_dir = get_test_replay_dir_path()
        backup_replay_dir = get_backup_test_replay_dir_path()
        setup_test_replay_db_dir(replay_dir)

        db = ReplayDB(replay_dir, backup_replay_dir)

        assert not len(db.get_current_backup())
        db.backup()
        assert db.compare_dir(replay_dir, backup_replay_dir.joinpath(db.get_current_backup())),\
            "The backup dir {} and replay dir {} are not same after backup.".format(backup_replay_dir.joinpath(db.is_backup_needed()), replay_dir)

        test_experiment_arr = get_test_experiment_arr()
        assert test_experiment_arr[0] in str(db.complete_experiment_arr[0]),\
            "Directory {} should have complete output file set.".format(db.complete_experiment_arr[0])

        assert test_experiment_arr[1] in str(db.live_experiment_arr[0]),\
            "Directory {} should have complete output file set.".format(db.live_experiment_arr[0])
        
        assert test_experiment_arr[2] in str(db.live_experiment_arr[1]),\
            "Directory {} should have complete output file set.".format(db.live_experiment_arr[1])
        
        prev_complete_experiment_count = len(db.complete_experiment_arr)
        make_experiment_complete(db.live_experiment_arr[0])
        assert test_experiment_arr[1] in str(db.complete_experiment_arr[1]),\
            "Directory {} should have complete output file set.".format(db.complete_experiment_arr[0])
        
        assert len(db.complete_experiment_arr) == prev_complete_experiment_count+1,\
            "The number of complete experiments changed after deleting dead experiments."

        db.backup()
        assert db.compare_dir(replay_dir, backup_replay_dir.joinpath(db.get_current_backup())),\
            "The backup dir {} and replay dir {} are not same after backup.".format(backup_replay_dir.joinpath(db.is_backup_needed()), replay_dir)
        
        db.delete_dead_experiments([])
        assert not len(db.live_experiment_arr),\
            "The directory {} of live experiment should be deleted.".format(db.live_experiment_arr[1])
        
        db.rm_dir(replay_dir)
        db.rm_dir(backup_replay_dir)
        
        
if __name__ == '__main__':
    main()