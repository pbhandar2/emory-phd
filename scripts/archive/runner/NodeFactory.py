"""NodeFactory manages remote nodes used for block trace replay. 

It performs functions like setting up the node, move replay output from
remote node to local, check for running experiment in a node and more. 

Usage:
    node_factory = NodeFactory(path/to/node/config/file)
    node = node_factory.get_free_node()
"""

from typing import Union 
from json import load, loads, dumps 
from pathlib import Path 

from keyuri.config.Config import GlobalConfig
from expK8.remoteFS.Node import Node, RemoteRuntimeError


class NodeFactory:
    def __init__(
            self, 
            node_config_file_path: Path
    ) -> None:
        """Create a factory that manages nodes that run block trace replay. 

        Args:
            node_config_file_path: Path to node configuration file. 
        
        Attributes:
            _config: Dictionary of information to connect to remote nodes. 
            _nodes: Array of Node objects that can be used to run commands in remote nodes. 
        """
        with node_config_file_path.open("r") as node_config_file_handle:
            self._config = load(node_config_file_handle)

        self._nodes = []
        for node_name in self._config["nodes"]:
            node_info = self._config["nodes"][node_name]
            node = Node(
                    node_info["host"], 
                    node_info["host"], 
                    self._config["creds"][node_info["cred"]], 
                    self._config["mounts"][node_info["mount"]])
            
            if not node._ssh_exception:
                self._nodes.append(node)
    

    def get_free_node_list(self) -> list:
        """Get a node that is ready to run block trace replay.
        
        Returns:
            node: Node object that is ready to run block trace replay or None. 
        """
        node_status_list = self.get_node_status()
        free_node_list = []
        for node_info in node_status_list:
            if not node_info["backing"] or not node_info["nvm"] or not node_info["cb"] or not node_info["cydonia"] or node_info["replay"]:
                continue 
            
            for node in self._nodes:
                if node_info["output"]:
                    self.transfer_complete_experiment(node)

                if node.host == node_info["host"]:
                    self.pre_replay_sanity_check(node)
                    free_node_list.append(node)
        
        return free_node_list
    

    def get_node_status(self) -> list:
        """Get the status of the nodes in the configuration file.
        
        Returns:
            status_df: DataFrame of node host names and its status values. 
        """
        status_arr = []
        for node in self._nodes:
            if node._ssh_exception:
                continue 

            # if replay is already running, we know the setup is good 
            if self.is_replay_running(node):
                status_arr.append({
                    "host": node.host, 
                    "machine": node.machine_name,
                    "backing": 1,
                    "nvm": 1,
                    "cb": 1,
                    "cydonia": 1,
                    "replay": 1,
                    "output": self.has_complete_replay(node)
                })
                continue 
            
            # check storage files need for replay exist and are of the required size 
            backing_storage_file_status = self.check_storage_file(node, "~/disk", "disk.file", 950000)
            nvm_storage_file_status = self.check_storage_file(node, "~/nvm", "disk.file", 10)

            cydonia_setup_status = 0 
            cachebench_test_status = 0  
            if backing_storage_file_status == 1:
                try:
                    if not self.test_cachebench(node):
                        self.install_cachebench(node) 
                    self.test_cachebench(node)
                    cachebench_test_status = 1

                    self.setup_cydonia(node)
                    cydonia_setup_status = 1
                except RemoteRuntimeError:
                    pass 

            status_arr.append({
                "host": node.host, 
                "machine": node.machine_name,
                "backing": backing_storage_file_status,
                "nvm": nvm_storage_file_status,
                "cb": cachebench_test_status,
                "cydonia": cydonia_setup_status,
                "replay": 0,
                "output": self.has_complete_replay(node)
            })
            print(dumps(status_arr, indent=2))
        return status_arr
    

    @staticmethod
    def has_complete_replay(
        node: Node,
        global_config: GlobalConfig = GlobalConfig()
    ) -> bool:
        return all([node.file_exists(output_file) for output_file in global_config.replay_output_file_list])


    @staticmethod
    def clone_cydonia(
        node: Node,
        dir_path: str = "~/disk/CacheLib/phdthesis"
    ) -> None:
        """Clone the cydonia repo. 

        Args:
            node: Node where cydonia repository should be cloned. 
            dir_path: Path of directory in remote node where cydonia is cloned. 
        """
        clone_cmd = "git clone https://github.com/pbhandar2/phdthesis {}".format(dir_path)
        stdout, stderr, exit_code = node.exec_command(clone_cmd.split(' '))
        if exit_code:
            raise RemoteRuntimeError(clone_cmd.split(' '), node.host, stdout, stderr, exit_code)


    @staticmethod
    def pre_replay_sanity_check(node: Node) -> None:
        """Make sure the packages and output directories are updated in the node before starting replay.
        
        Args:
            node: Node where replay will be run.
        """
        # make sure that we have permission to track power usage 
        node.set_perm_for_power_tracking()

        # make sure we create and own the output directory 
        node.chown("/run")
        node.mkdir("/run/replay")
        node.chown("/run/replay")

        # make sure we have the most updated packages 
        NodeFactory.setup_cydonia(node)
        NodeFactory.update_cachebench_repo(node)


    @staticmethod
    def setup_cydonia(node: Node) -> int:
        """Setup the cydonia package in the node."""
        repo_dir = "~/disk/CacheLib/phdthesis"
        cydonia_dir = "{}/cydonia".format(repo_dir)
        if not node.dir_exists(cydonia_dir):
            node.rm(repo_dir)
            NodeFactory.clone_cydonia(node)

        change_cydonia_dir = "cd {}; ".format(cydonia_dir)
        pull_cmd = "git pull origin main; "
        install_cmd = "pip3 install . --user"
        final_cmd = change_cydonia_dir + pull_cmd + install_cmd

        _, _, exit_code = node.exec_command(final_cmd.split(' '))
        return 0 if exit_code else 1 


    @staticmethod
    def update_cachebench_repo(node: Node) -> None:
        """Update the cachebench repo to include the most recent code."""
        install_cachebench_cmd = "cd ~/disk/CacheLib; git pull origin active; sudo ./contrib/build.sh -j -d"
        stdout, stderr, exit_code = node.exec_command(install_cachebench_cmd.split(' '))
        if exit_code:
            raise RemoteRuntimeError(install_cachebench_cmd.split(' '), node.host, stdout, stderr, exit_code)


    @staticmethod
    def install_cachebench(node: Node) -> None:
        """Install cachebench in a node."""
        install_linux_packages_cmd = str("sudo apt-get update; sudo apt install -y libaio-dev python3-pip")
        stdout, stderr, exit_code = node.exec_command(install_linux_packages_cmd.split(' '))
        if exit_code:
            raise RemoteRuntimeError(install_linux_packages_cmd.split(' '), node.host, stdout, stderr, exit_code)
        
        cachelib_dir = "~/disk/CacheLib"
        if not node.dir_exists(cachelib_dir):
            clone_cachebench = "git clone https://github.com/pbhandar2/CacheLib.git ~/disk/CacheLib"
            checkout_cachebench = "git -C ~/disk/CacheLib/ checkout active"
            install_cachebench_cmd = "{};{}".format(clone_cachebench, checkout_cachebench)
            stdout, stderr, exit_code = node.exec_command(install_cachebench_cmd.split(' '))
            if exit_code:
                raise RemoteRuntimeError(install_cachebench_cmd.split(' '), node.host, stdout, stderr, exit_code)
        
        NodeFactory.update_cachebench_repo(node)
    

    @staticmethod
    def test_cachebench(node: Node) -> None:
        cachelib_dir = "~/disk/CacheLib"
        test_config_file_path = "~/disk/CacheLib/cachelib/cachebench/test_configs/block_replay/sample_config.json"
        change_cachelib_dir = "cd {};".format(cachelib_dir)
        cachebench_binary_path = "./opt/cachelib/bin/cachebench"
        cachelib_cmd = "{} {} --json_test_config {}".format(
                        change_cachelib_dir,
                        cachebench_binary_path,
                        test_config_file_path)

        stdout, stderr, exit_code = node.exec_command(cachelib_cmd.split(' '))
        if exit_code:
            raise RemoteRuntimeError(cachelib_cmd.split(' '), node.host, stdout, stderr, exit_code)


    @staticmethod
    def is_replay_running(node: Node) -> bool:
        """Check if replay is running in a node.
        
        Args:
            node: Node to be checked. 
        
        Returns:
            running: Boolean indicating if trace replay is already running in the node. 
        """
        running = False 
        ps_output = node.ps()
        for ps_row in ps_output.split('\n'):
            if "bin/cachebench" in ps_row:
                running = True 
                break 
        return running
    

    @staticmethod
    def get_local_block_trace_path(
        remote_block_trace_path: str,
        config: GlobalConfig = GlobalConfig()
    ) -> Path:
        """Get local block trace path given the remote block trace path.
        
        Args:
            remote_block_trace_path: Path of trace in remote node. 
        
        Returns:
            local_block_trace_path: Local path of trace. 
        """
        remote_block_trace_file_name = Path(remote_block_trace_path).stem
        split_file_name = remote_block_trace_file_name.split("-")
        if len(split_file_name) > 2:
            workload_type, workload_name, sample_type, sample_params = split_file_name
            local_block_trace_path = config.sample_block_trace_dir_path.joinpath(sample_type, workload_type, workload_name, "{}.csv".format(sample_params))
        else:
            workload_type, workload_name = split_file_name
            local_block_trace_path = config.block_trace_dir_path.joinpath(workload_type, "{}.csv".format(workload_name))
        return local_block_trace_path


    @staticmethod
    def get_replay_params(node: Node) -> dict:
        """Get the replay parameters from the configuration file in the node.
        
        Args:
            node: Node from which replay parameters are extracted.
        
        Returns:
            params: Dictionary of replay parameters.
        """
        config_json_str = node.cat("/run/replay/config.json")
        config_json = loads(config_json_str)
        cache_config = config_json["cache_config"]
        replay_config = config_json["test_config"]["blockReplayConfig"]
        
        replay_params = {}
        replay_params["num_async_threads"] = replay_config["asyncIOReturnTrackerThreads"]
        replay_params["num_block_threads"] = replay_config["blockRequestProcesserThreads"]
        replay_params["max_pending_block_req_count"] = replay_config["maxPendingBlockRequestCount"]
        replay_params["t1_size_mb"] = cache_config["cacheSizeMB"]

        replay_params["t2_size_mb"] = 0 
        if "nvmCacheSizeMB" in cache_config:
            replay_params["t2_size_mb"] = cache_config["nvmCacheSizeMB"]
        replay_params["replay_rate"] = 1
        if "replayRate" in replay_config:
            replay_params["replay_rate"] = replay_config["replayRate"]
        replay_params["iteration"] = 0 
        if "iteration" in replay_config:
            replay_params["iteration"] = replay_config["iteration"]
        replay_params["block_trace_path"] = NodeFactory.get_local_block_trace_path(replay_config["traces"][0])

        block_trace_file_name = Path(replay_config["traces"][0]).stem
        if len(block_trace_file_name.split("-")) > 2:
            replay_params["sample"] = 1 
        else:
            replay_params["sample"] = 0 
        
        return replay_params


    @staticmethod
    def get_remote_block_trace_path(replay_params: dict) -> str:
        """Get the path of block trace file during trace replay given its parameters. 

        Args:
            replay_params: Dictionary of replay parameters. 

        Returns:
            remote_block_trace_path: Path of block trace in remote node where replay is run. 
        """
        block_trace_path = Path(replay_params["block_trace_path"])
        if replay_params['sample']:
            sample_params_str = block_trace_path.stem 
            workload_name = block_trace_path.parent.name 
            workload_type = block_trace_path.parent.parent.name 
            sample_type = block_trace_path.parent.parent.parent.name
            remote_file_name = "{}-{}-{}-{}.csv".format(workload_type, workload_name, sample_type, sample_params_str)
        else:
            workload_name = block_trace_path.stem 
            workload_type = block_trace_path.parent.name 
            remote_file_name = "{}-{}.csv".format(workload_type, workload_name)
        
        return "/run/replay/{}".format(remote_file_name)


    @staticmethod
    def transfer_block_trace(
            node: Node,
            replay_params: dict 
    ) -> str:
        """Transfer block trace to remote node for replay. 
        
        Args:
            node: Remote node to transfer block trace to. 
            replay_params: Dictionary of replay parameters. 
        
        Returns:
            remote_block_trace_path: Path in remote node where block trace was transfered.
        """

        local_block_trace_size_byte = Path(replay_params["block_trace_path"]).expanduser().stat().st_size
        remote_block_trace_path = NodeFactory.get_remote_block_trace_path(replay_params)
        remote_block_trace_size_byte = node.get_file_size(remote_block_trace_path)

        if local_block_trace_size_byte != remote_block_trace_size_byte:
            node.scp(replay_params["block_trace_path"], remote_block_trace_path)
        
        remote_block_trace_size_byte = node.get_file_size(remote_block_trace_path)
        assert local_block_trace_size_byte == remote_block_trace_size_byte, \
            "Remote {} and local {} block trace have differnet sizes.".format(remote_block_trace_size_byte, local_block_trace_size_byte)

        return remote_block_trace_path


    @staticmethod
    def transfer_complete_experiment(
            node: Node,
            config: GlobalConfig = GlobalConfig()
    ) -> bool:
        """Transfer replay output from a remote node.
        
        Args:
            node: Node where replay was run.
        
        Returns:
            transfer_done: Boolean indicating if data was transfered from remote node to local. 
        """
        transfer_done = False 
        replay_params = NodeFactory.get_replay_params(node)
        replay_output_dir = config.get_replay_output_dir_path(node.machine_name, replay_params)
        replay_output_file_status = [node.file_exists(output_file) for output_file in config.replay_output_file_list]
        if all(replay_output_file_status):
            for remote_output_file_path in config.replay_output_file_list:
                remote_output_file_name = Path(remote_output_file_path).name
                local_output_file_path = replay_output_dir.joinpath(remote_output_file_name)

                remote_file_size_byte = node.get_file_size(remote_output_file_path)
                local_file_size_byte = 0 
                if local_output_file_path.exists():
                    local_file_size_byte = local_output_file_path.stat().st_size

                if remote_file_size_byte != local_file_size_byte:
                    print("File size {},{} did not match. Downloading {} to {}".format(
                                                                                remote_file_size_byte, 
                                                                                local_file_size_byte, 
                                                                                remote_output_file_path, 
                                                                                local_output_file_path))
                    node.download(remote_output_file_path, str(local_output_file_path.expanduser()))
                
                remote_file_size_byte = node.get_file_size(remote_output_file_path)
                local_file_size_byte = 0 
                if local_output_file_path.exists():
                    local_file_size_byte = local_output_file_path.stat().st_size

                assert remote_file_size_byte == local_file_size_byte, \
                    "The output file size of remote file {} {} and local file {} {} did not match.".format(
                                                                                                    remote_output_file_path,
                                                                                                    remote_file_size_byte,
                                                                                                    local_output_file_path, 
                                                                                                    local_file_size_byte)
                node.rm(remote_output_file_path)
            transfer_done = True 
        
        return transfer_done
    

    @staticmethod
    def check_storage_file(
        node: Node,
        mountpoint: str,
        path_relative_to_mountpoint: str,
        storage_file_size_mb: int 
    ) -> int:
        """Check if a required storage file is correctly setup and create one otherwise.

        Args:
            node: Node where file is checked. 
            mountpoint: Mountpoint where file is created. 
            path_relative_to_mountpoint: Path relative to mountpoint of the storage file. 
            storage_file_size_mb: Minimum size of file in MB

        Returns:
            status: Status of storage files represented by values: -1 (creation is running), 0 (no valid mount found),
                        1 (done), 2(started creation)
        """
        mount_info = node.get_mountpoint_info(mountpoint)
        if not mount_info:
            return 0 
        
        create_file_ps_row = None
        ps_output = node.ps()
        for ps_row in ps_output.split('\n'):
            if "dd" not in ps_row:
                continue 
            
            if node.format_path(mountpoint) not in ps_row:
                continue 
            
            if path_relative_to_mountpoint not in ps_row:
                continue 
            
            create_file_ps_row = ps_row 
            break 
        
        create_file_live = create_file_ps_row is not None 
        if create_file_live:
            return -1 
        else:
            file_path = "{}/{}".format(mountpoint, path_relative_to_mountpoint)
            current_file_size_byte = node.get_file_size("{}/{}".format(mountpoint, path_relative_to_mountpoint))
            if current_file_size_byte//(1024*1024) < storage_file_size_mb:
                print("current file size {} and min file size {}".format(current_file_size_byte//(1024*1024), storage_file_size_mb))
                node.create_random_file_nonblock(file_path, storage_file_size_mb)
                return 2
            else:
                return 1


def main():
    factory = NodeFactory(Path("config.json"))
    factory.get_node_status()


if __name__ == "__main__":
    main()