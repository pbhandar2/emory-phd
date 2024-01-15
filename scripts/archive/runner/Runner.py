from pathlib import Path 

from NodeFactory import NodeFactory
from ExperimentFactory import ExperimentFactory
from expK8.remoteFS.Node import Node 


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


def get_replay_cmd(
        remote_block_trace_path: str, 
        t1_size_mb: int, 
        t2_size_mb: int, 
        replay_rate: int 
) -> str:
    replay_cmd = "nohup python3 ~/disk/CacheLib/phdthesis/scripts/fast24/TraceReplay.py "
    replay_cmd += "{} ".format(remote_block_trace_path)
    replay_cmd += "{} ".format(int(t1_size_mb))
    if t2_size_mb > 0:
        replay_cmd += " --t2_size_mb {}".format(int(t2_size_mb))
    
    if replay_rate > 1:
        replay_cmd += " --replay_rate {}".format(int(replay_rate))
    
    replay_cmd += " >> /run/replay/replay.log 2>&1"
    return replay_cmd


def run_block_trace_replay(
        node: Node,
        replay_params: dict
) -> None:
    """Run trace replay with the given paramaters and node. 
    
    Args:
        node: Node where trace replay is to be run. 
        replay_params: Dictionary of replay parameters. 
    """
    remote_block_trace_path = transfer_block_trace(node, replay_params)
    assert node.file_exists(remote_block_trace_path), "Remote block trace path does not exist."
    replay_cmd_str = get_replay_cmd(
                        remote_block_trace_path, 
                        replay_params["t1_size_mb"], 
                        replay_params["t2_size_mb"],
                        replay_params["replay_rate"])
    print("Node: {}, replay_cmd: {}".format(node.host.split('.')[0], replay_cmd_str))
    node.nonblock_exec_cmd(replay_cmd_str)


def main():
    node_factory = NodeFactory(Path("config.json"))
    experiment_factory = ExperimentFactory()
    free_node_list = node_factory.get_free_node_list()

    for free_node in free_node_list:
        print("free node: {}".format(free_node.host, free_node.machine_name))
        pending_replay_arr = experiment_factory.get_pending_replay_arr(free_node.machine_name) 
        print("replay: {}".format(pending_replay_arr[0]))

        if len(pending_replay_arr) > 0:
            experiment_factory.init_output_path(free_node.machine_name, pending_replay_arr[0])
            run_block_trace_replay(free_node, pending_replay_arr[0])
            break 

if __name__ == "__main__":
    main()