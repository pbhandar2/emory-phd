
from keyuri.config.Config import BaseMTExperiment, GlobalConfig, NodeConfig
from keyuri.analysis.ReplayDB import ReplayDB


class ExperimentFactory:
    def __init__(
        self,
        global_config: GlobalConfig = GlobalConfig(),
        node_config: NodeConfig = NodeConfig()
    ) -> None:
        self._base_mt_config = BaseMTExperiment()
        self._global_config = global_config
        self._node_config = node_config
        self._db = ReplayDB(self._global_config.replay_dir_path, self._global_config.replay_backup_dir_path)


    def get_pending_replay_arr(self, machine_name: str) -> list:
        """Get array of pending replay for a given machine. 

        Args:
            machine_name: Name of the machine for which we are tracking replay. 
        
        Returns:
            pending_replay_arr: Array of configuration of pending replays. 
        """
        pending_replay_arr = []
        replay_config_arr = self._base_mt_config.get_all_replay_config()
        for replay_config in replay_config_arr:
            if replay_config["t1_size_mb"] < self._node_config.machine_dict[machine_name]["min_t1_size_mb"]:
                continue 

            if replay_config["t2_size_mb"] < self._node_config.machine_dict[machine_name]["min_t2_size_mb"]:
                continue 

            if not self._db.has_started(machine_name, replay_config):
                pending_replay_arr.append(replay_config)
            
        return pending_replay_arr
    

    def init_output_path(self, machine_name: str, replay_params: dict) -> None:
        """Init output directory for machine name and replay parameters. 

        Args:
            machine_name: Name of the machine. 
            replay_params: Dictionary of replay parameters. 
        """
        return self._db.get_output_path(machine_name, replay_params).mkdir(exist_ok=True, parents=True)
