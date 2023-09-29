""" This script generates cache trace in remote nodes. 

python3 generate.py 
/research2/mtc/cp_traces/pranav/postprocess_sample_block_traces/reduce-12/iat/cp/w18/1_12_42.csv 
/research2/mtc/cp_traces/pranav/postprocess_sample_cache_traces/reduce-12/iat/cp/w18/1_12_42.csv
"""

from pathlib import Path 
from argparse import ArgumentParser 

from expK8.remoteFS.NodeFactory import NodeFactory
from keyuri.config.Config import GlobalConfig, SampleExperimentConfig
from Setup import Setup 


def main():
    parser = ArgumentParser(description="Generate a cache trace in a remote node.")
    parser.add_argument("workload_name", type=str, help="Name of the workload")
    parser.add_argument("--workload_type", type=str, default="cp", help="Type of workload.")
    parser.add_argument("--sample_type", type=str, default="iat", help="Type of sample.")
    parser.add_argument("--algo_type", type=str, default="reduce-12", help="Type of algorithm used for post-processing.")
    parser.add_argument("--trace_type", type=str, default="cache", help="Type of trace to generate cache or access.")
    args = parser.parse_args()

    node_factory = NodeFactory("config.json")

    global_config = GlobalConfig()
    sample_trace_dir_path = global_config.postprocess_sample_block_trace_dir_path.joinpath(args.algo_type,
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
        return 
    
    for blk_trace_path in sample_trace_dir_path.iterdir():
        if args.trace_type == "cache":
            cache_trace_path = cache_trace_dir_path.joinpath(blk_trace_path.name)
        else:
            cache_trace_path = access_trace_dir_path.joinpath(blk_trace_path.name)

        sample_metadata_file_path = sample_metadata_dir_path.joinpath(blk_trace_path.name)

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
        create_cache_trace_cmd = "{}; python3 runner.py {} {} {} --trace_type".format(change_dir_cmd, 
                                                                        remote_blk_trace_path, 
                                                                        remote_cache_trace_path,
                                                                        stack_binary_path,
                                                                        args.trace_type)

        stdout, stdin, exit_code = cur_node.exec_command(create_cache_trace_cmd.split(' '))
        if exit_code == 0:
            cache_trace_path.parent.mkdir(exist_ok=True, parents=True)
            cur_node.download(remote_cache_trace_path, str(cache_trace_path.absolute()))
            print("Downloaded {} to {}".format(remote_cache_trace_path, cache_trace_path))

if __name__ == "__main__":
    main()