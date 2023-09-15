"""Runner config reads cloudlab node logs to generate a config file with node information 
needed to run block trace replay. 

Usage:
    python3 RunnerConfig.py --cloudlab_data_dir /path/to/dir/with/cloudlab/node/logs --output_path /path/to/node/config.json
"""

from json import load, dump 
from pathlib import Path 
from argparse import ArgumentParser


class RunnerConfig:
    def __init__(
            self, 
            config_dir_path: Path
    ) -> None:
        self._config_dir = config_dir_path

        with self._config_dir.joinpath("mounts.json").open("r") as mounts_file_handle:
            self._mounts = load(mounts_file_handle)

        with self._config_dir.joinpath("creds.json").open("r") as creds_file_handle:
            self._creds = load(creds_file_handle)
    

    def generate_config_file(
            self, 
            output_path: Path 
    ) -> None:
        """Generate a configuration file based on the current configuration
        files. 

        Args:
            output_path: Path where the configuration file will be generated. 
        """
        node_dict = {}
        for config_file_path in self._config_dir.iterdir():
            if ".log" not in config_file_path.name:
                continue 

            with config_file_path.open("r") as config_file_handle:
                log_line = config_file_handle.readline()
                while log_line:
                    if "emulab:vnode" not in log_line:
                        log_line = config_file_handle.readline()
                        continue 
                    """
                    Sample line being parsed to get node information:
                    emulab:vnode name="c220g1-031124" hardware_type="c220g1"/><host name="node3.cachelib-1.microthreads-PG0.wisc.cloudlab.us" 
                    ipv4="128.105.146.1"/><services><login authentication="ssh-keys" hostname="c220g1-031124.wisc.cloudlab.us" port="22" 
                    username="vishwa"/><login authentication="ssh-keys" hostname="c220g1-031124.wisc.cloudlab.us" port="22" username="yazhuoz"/>
                    <login authentication="ssh-keys" hostname="c220g1-031124.wisc.cloudlab.us" port="22" username="nikbapat"/>
                    <login authentication="ssh-keys" hostname="c220g1-031124.wisc.cloudlab.us" port="22" username="ymir"/>
                    <login authentication="ssh-keys" hostname="c220g1-031124.wisc.cloudlab.us" port="22" username="mohsal"/>
                    <login authentication="ssh-keys" hostname="c220g1-031124.wisc.cloudlab.us" port="22" username="jackzzzy"/>
                    <login authentication="ssh-keys" hostname="c220g1-031124.wisc.cloudlab.us" port="22" username="geleta"/>
                    <emulab:console server="boss.wisc.cloudlab.us"/>
                    <emulab:recovery available="true"/>
                    <emulab:powercycle available="true"/>
                    <emulab:imageable available="true"/></services></node>
                    """
                    host_name = log_line.split("hostname=")[1].split(' ')[0].replace('"', '')
                    name = log_line.split("host name=")[1].split(' ')[0].replace('"', '')
                    machine_type = host_name.split("-")[0]
                    node_name = "{}_{}".format(config_file_path.stem, name.split(".")[0])
                    cred_name = "cloudlab" if "pbhandar" in log_line else "cloudlab_vishwa"
                    node_dict[node_name] = {
                        "host": host_name,
                        "cred": cred_name,
                        "mount": machine_type
                    }
                    print(node_dict)
                    log_line = config_file_handle.readline()
        
        node_config = {
            "creds": self._creds,
            "mounts": self._mounts,
            "nodes": node_dict
        }
        with output_path.open("w+") as output_file_handle:
            dump(node_config, output_file_handle, indent=2)


def main():
    parser = ArgumentParser(description="Generate configuration file to run block trace replay in remote nodes.")
    parser.add_argument("--cloudlab_data_dir",
                        type=Path,
                        default=Path("./cloud"),
                        help="Path to directory containing all files needed to generate configuration file.")
    parser.add_argument("--output_path", 
                        type=Path, 
                        default=Path("config.json"), 
                        help="Path to configuration file containing node details.")
    args = parser.parse_args()

    config = RunnerConfig(args.cloudlab_data_dir)
    print("Config object created.")
    config.generate_config_file(args.output_path)


if __name__ == "__main__":
    main()