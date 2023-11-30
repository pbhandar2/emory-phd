"""This script generates cache trace in remote nodes."""

from pathlib import Path
from random import sample 
from typing import Optional
from argparse import ArgumentParser 

from Setup import Setup 
from keyuri.config.Config import GlobalConfig, SampleExperimentConfig
from expK8.remoteFS.NodeFactory import NodeFactory
from expK8.remoteFS.Node import RemoteRuntimeError, Node 


def parse_args():
    global_config = GlobalConfig()
    parser = ArgumentParser(description="Compute cache trace from block trace in remote node and transfer it back to local.")
    parser.add_argument("workload_name", 
                            type=str, 
                            help="Name of the workload.")

    parser.add_argument("--workload_type", 
                            type=str, 
                            default="cp", 
                            help="Type of workload.")

    parser.add_argument("--sample_type", 
                            type=str, 
                            help="Type of sample.")

    parser.add_argument("--algo_type", 
                            type=str, 
                            help="Type of algorithm used for post-processing.")

    parser.add_argument("--trace_type", 
                            type=str, 
                            default="cache", 
                            help="Type of trace to generate cache or access.")

    parser.add_argument("--source_dir",
                            type=Path,
                            default=global_config.source_dir_path,
                            help="The source directory for all data.")

    parser.add_argument("--node_config_file",
                            type=Path,
                            default=Path("config.json"),
                            help="Path to file with configurations of remote nodes.")

    return parser.parse_args()


def get_free_node(node_config_file_path: Path) -> Optional[Node]:
    node_factory = NodeFactory(str(node_config_file_path))
    free_node = None
    for node_index in range(len(node_factory.nodes)):
        cur_node = node_factory.nodes[node_index]
        setup = Setup(cur_node)
        setup_ready = setup.setup()

        if setup_ready:
            free_node = cur_node
            break 
    return free_node 


def generate_cache_trace(
        block_trace_path: Path, 
        cache_trace_path: Path, 
        node: Node, 
        remote_source_dir_str: str = "~/nvm", 
        trace_type: str = "cache"
) -> None:
    remote_source_dir_str = node.format_path(remote_source_dir_str)
    remote_blk_trace_dir = "{}/{}".format(remote_source_dir_str, str(block_trace_path.parent.expanduser()))
    remote_cache_trace_dir = "{}/{}".format(remote_source_dir_str, str(cache_trace_path.parent.expanduser()))

    node.mkdir(remote_blk_trace_dir)
    remote_blk_trace_path = "{}/{}".format(remote_blk_trace_dir, block_trace_path.name)
    remote_cache_trace_path = "{}/{}".format(remote_cache_trace_dir, block_trace_path.name)

    print(str(block_trace_path), remote_blk_trace_path)

    node.scp(str(block_trace_path), remote_blk_trace_path)
    assert node.file_exists(remote_blk_trace_path), \
        "Block trace was not transfered to remote node."

    change_dir_cmd = "cd ~/disk/emory-phd/scripts/cache_trace"
    stack_binary_path = "~/disk/emory-phd/modules/Cydonia/scripts/stack-distance/stack-distance"
    create_cache_trace_cmd = "{}; python3 runner.py {} {} {} --trace_type {}".format(change_dir_cmd, 
                                                                    remote_blk_trace_path, 
                                                                    remote_cache_trace_path,
                                                                    stack_binary_path,
                                                                    trace_type)

    print("Running cmd: {}".format(create_cache_trace_cmd))
    stdout, stdin, exit_code = node.exec_command(create_cache_trace_cmd.split(' '))
    print(stdin, stdout, exit_code)
    if exit_code == 0:
        cache_trace_path.parent.mkdir(exist_ok=True, parents=True)
        node.download(remote_cache_trace_path, str(cache_trace_path.absolute()))
        print("Downloaded {} to {}".format(remote_cache_trace_path, cache_trace_path))
    else:
        RemoteRuntimeError(create_cache_trace_cmd.split(' '), node.host, exit_code, stdout, stdin)
    

def main():
    args = parse_args()

    sample_config = SampleExperimentConfig()
    global_config = GlobalConfig()
    if args.source_dir != global_config.source_dir_path:
        global_config = GlobalConfig(source_dir_path=args.source_dir)

    free_node = get_free_node(args.node_config_file)
    if free_node is None:
        print("No node available to generate cache trace.")
        return 
    
    if args.sample_type is None and args.algo_type is None:
        if args.trace_type == "cache":
            block_trace_path = global_config.get_block_trace_path(args.workload_type, args.workload_name)
            cache_trace_path = global_config.get_block_cache_trace_path(args.workload_type, args.workload_name)
        else:
            block_trace_path = global_config.get_access_trace_path(args.workload_type, args.workload_name)
            cache_trace_path = global_config.get_access_cache_trace_path(args.workload_type, args.workload_name)

        if cache_trace_path.exists():
            print("Cache trace path {} already exists!".format(cache_trace_path))
        else:
            print("Generating {}.".format(cache_trace_path))
            generate_cache_trace(block_trace_path, cache_trace_path, free_node)
    elif args.sample_type == "iat" and args.algo_type is None:
        if args.trace_type == "cache":
            block_trace_dir_path = global_config.sample_block_trace_dir_path.joinpath(args.sample_type, args.workload_type, args.workload_name)
            cache_trace_dir_path = global_config.sample_cache_trace_dir_path.joinpath(args.sample_type, args.workload_type, args.workload_name)
        else:
            block_trace_dir_path = global_config.sample_block_access_trace_dir_path.joinpath(args.sample_type, args.workload_type, args.workload_name)
            cache_trace_dir_path = global_config.sample_block_access_cache_trace_dir_path.joinpath(args.sample_type, args.workload_type, args.workload_name)
        
        for block_trace_path in block_trace_dir_path.iterdir():
            cache_trace_path = cache_trace_dir_path.joinpath(block_trace_path.name)
            if cache_trace_path.exists():
                print("Cache trace path {} already exists!".format(cache_trace_path))
            else:
                print("Generating {}.".format(cache_trace_path))
                generate_cache_trace(block_trace_path, cache_trace_path, free_node)






    # sample_trace_dir_path = global_config.sample_block_trace_dir_path.joinpath(args.sample_type, 
    #                                                                             args.workload_type, 
    #                                                                             args.workload_name)
    
    # sample_cache_trace_dir_path = global_config.sample_cache_trace_dir_path.joinpath(args.sample_type, 
    #                                                                                     args.workload_type, 
    #                                                                                     args.workload_name)
    
    # # first make sure all the samples have cache traces 
    # for sample_trace_path in sample_trace_dir_path.iterdir():
    #     cache_trace_path = sample_cache_trace_dir_path.joinpath(sample_trace_path.name)
    #     if cache_trace_path.exists():
    #         print("Cache trace {} exists!".format(cache_trace_path))
    #         continue 

    #     print("Generating cache trace {}".format(cache_trace_path))
        



    return 

    post_process_sample_trace_dir_path = global_config.postprocess_sample_block_trace_dir_path.joinpath(args.algo_type,
                                                                                                            args.sample_type,
                                                                                                            args.workload_type,
                                                                                                            args.workload_name)


    cache_trace_dir_path = global_config.postprocess_sample_cache_trace_dir_path.joinpath(args.algo_type,
                                                                                                args.sample_type,
                                                                                                args.workload_type,
                                                                                                args.workload_name)
    
    access_trace_dir_path = global_config.postprocess_sample_block_access_trace_dir_path.joinpath(args.algo_type,
                                                                                                    args.sample_type,
                                                                                                    args.workload_type,
                                                                                                    args.workload_name)
    
    sample_metadata_dir_path = global_config.postprocess_stat_dir_path.joinpath(args.algo_type,
                                                                                    args.sample_type,
                                                                                    args.workload_type,
                                                                                    args.workload_name)
    
    cur_node = node_factory.nodes[0]
    setup = Setup(cur_node)
    setup_ready = setup.setup()
    if not setup_ready:
        print("Node not ready.")
        return 
    
    for blk_trace_path in sample_trace_dir_path.iterdir():
        if args.trace_type == "cache":
            cache_trace_path = cache_trace_dir_path.joinpath(blk_trace_path.name)
        else:
            cache_trace_path = access_trace_dir_path.joinpath(blk_trace_path.name)
        
        if cache_trace_path.exists():
            print("Cache trace already exists {}.".format(cache_trace_path))
            continue 

        sample_metadata_file_path = sample_metadata_dir_path.joinpath(blk_trace_path.name.replace(".csv", ".json"))
        if not sample_metadata_file_path.exists():
            print("Sample metadata file does not exist {}".format(sample_metadata_file_path))
            continue 

        remote_blk_trace_dir = "{}/{}".format(setup._remote_source_dir_str, str(blk_trace_path.parent.expanduser()))
        remote_cache_trace_dir = "{}/{}".format(setup._remote_source_dir_str, str(cache_trace_path.parent.expanduser()))

        cur_node.mkdir(remote_blk_trace_dir)# transfer the block trace 
        remote_blk_trace_path = "{}/{}".format(remote_blk_trace_dir, blk_trace_path.name)
        remote_cache_trace_path = "{}/{}".format(remote_cache_trace_dir, blk_trace_path.name)

        cur_node.scp(blk_trace_path, remote_blk_trace_path)
        assert cur_node.file_exists(remote_blk_trace_path), \
            "Block trace was not transfered to remote node."

        change_dir_cmd = "cd ~/disk/emory-phd/scripts/cache_trace"
        stack_binary_path = "~/disk/emory-phd/modules/Cydonia/scripts/stack-distance/stack-distance"
        create_cache_trace_cmd = "{}; python3 runner.py {} {} {} --trace_type {}".format(change_dir_cmd, 
                                                                        remote_blk_trace_path, 
                                                                        remote_cache_trace_path,
                                                                        stack_binary_path,
                                                                        args.trace_type)

        print("Running cmd: {}".format(create_cache_trace_cmd))
        stdout, stdin, exit_code = cur_node.exec_command(create_cache_trace_cmd.split(' '))
        print(stdin, stdout, exit_code)
        if exit_code == 0:
            cache_trace_path.parent.mkdir(exist_ok=True, parents=True)
            cur_node.download(remote_cache_trace_path, str(cache_trace_path.absolute()))
            print("Downloaded {} to {}".format(remote_cache_trace_path, cache_trace_path))
        else:
            RemoteRuntimeError(create_cache_trace_cmd.split(' '), cur_node.host, exit_code, stdout, stdin)


if __name__ == "__main__":
    main()