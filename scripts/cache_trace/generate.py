""" This script generates cache trace in remote nodes. 

python3 generate.py 
/research2/mtc/cp_traces/pranav/postprocess_sample_block_traces/reduce-12/iat/cp/w18/1_12_42.csv 
/research2/mtc/cp_traces/pranav/postprocess_sample_cache_traces/reduce-12/iat/cp/w18/1_12_42.csv
"""

from pathlib import Path 
from argparse import ArgumentParser 

from expK8.remoteFS.NodeFactory import NodeFactory

from Setup import Setup 


def main():
    parser = ArgumentParser(description="Generate a cache trace in a remote node.")
    parser.add_argument("blk_trace_path", type=Path, help="Path to the block trace.")
    parser.add_argument("cache_trace_path", type=Path, help="Path to the block trace.")
    args = parser.parse_args()

    node_factory = NodeFactory("config.json")

    cur_node = node_factory.nodes[0]
    setup = Setup(cur_node, args.blk_trace_path, args.cache_trace_path)
    setup_ready = setup.setup()

    remote_blk_trace_dir = "{}/{}".format(setup._remote_source_dir_str, str(args.blk_trace_path.parent.expanduser()))
    remote_cache_trace_dir = "{}/{}".format(setup._remote_source_dir_str, str(args.cache_trace_path.parent.expanduser()))

    cur_node.mkdir(remote_blk_trace_dir)# transfer the block trace 
    remote_blk_trace_path = "{}/{}".format(remote_blk_trace_dir, args.blk_trace_path.name)
    remote_cache_trace_path = "{}/{}".format(remote_cache_trace_dir, args.blk_trace_path.name)

    cur_node.scp(args.blk_trace_path, remote_blk_trace_path)

    assert cur_node.file_exists(remote_blk_trace_path), \
        "Block trace was not transfered to remote node."

    change_dir_cmd = "cd ~/disk/emory-phd/scripts"
    stack_binary_path = "~/disk/emory-phd/modules/Cydonia/scripts/stack-distance/stack-distance"
    create_cache_trace_cmd = "{}; python3 generate.py {} {} {}".format(change_dir_cmd, 
                                                                       remote_blk_trace_path, 
                                                                       remote_cache_trace_path,
                                                                       stack_binary_path)

    stdout, stdin, exit_code = cur_node.exec_command(create_cache_trace_cmd.split(' '))

    print(stdin)
    print(stdout)
    print(exit_code)

    if exit_code == 0:
        args.cache_trace_path.mkdir(exists_ok=True, parents=True)
        cur_node.download(remote_cache_trace_path, str(args.cache_trace_path.absolute()))

        print("Downloaded {} to {}".format(remote_cache_trace_path, args.cache_trace_path))


if __name__ == "__main__":
    main()