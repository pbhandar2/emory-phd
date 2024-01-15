"""Setup the necessary files and code to generate the cache trace in remote node and transfer it back to local."""

from json import load 
from pathlib import Path 

from expK8.remoteFS.Node import Node
from expK8.remoteFS.NodeFactory import NodeFactory


class Setup:
    def __init__(
            self, 
            node: Node
    ) -> None:
        self._node = node 
        self._remote_source_dir_str = self._node.format_path("~/nvm")
        self._remote_repo_dir_str = "~/disk/emory-phd"
    

    def install_cmake(self) -> int:
        install_cmd = "sudo apt-get update; sudo apt install -y clang python3-pip"
        _, _, exit_code = self._node.exec_command(install_cmd.split(' '))
        return exit_code


    def clone_repo(self) -> int:
        if not self._node.dir_exists(self._remote_repo_dir_str):
            git_clone_cmd = "git clone https://github.com/pbhandar2/emory-phd {}".format(self._remote_repo_dir_str)
            full_cmd = "{}; cd {}; git submodule init; git submodule update".format(git_clone_cmd, self._remote_repo_dir_str)
            _, _, exit_code = self._node.exec_command(full_cmd.split(' '))
            return exit_code
        else:
            update_cmd = "cd {}; git pull origin main; pip3 install . --user;".format(self._remote_repo_dir_str)
            stdin, stdout, exit_code = self._node.exec_command(update_cmd.split(' '))
            print(stdout)
            return exit_code
    

    def update_repo(self) -> int:
        cydonia_repo_dir = "cd {}/modules/Cydonia/".format(self._remote_repo_dir_str)
        cydonia_update_cmd = "{}; git submodule init; git submodule update; pip3 install . --user;".format(cydonia_repo_dir)
        _, _, exit_code = self._node.exec_command(cydonia_update_cmd.split(' '))
        return exit_code
    

    

    def setup_stack_distance(self) -> None:
        stack_distance_dir = "cd {}/modules/Cydonia/scripts/stack-distance".format(self._remote_repo_dir_str)
        install_cmd = "{}; sudo make".format(stack_distance_dir)
        _, _, exit_code = self._node.exec_command(install_cmd.split(' '))
        return exit_code
    

    def setup(self) -> bool:
        cmake_install_exit_code = self.install_cmake()
        print("Cmake install: {}".format(cmake_install_exit_code))

        clone_repo_exit_code = self.clone_repo()
        print("Clone repo: {}".format(clone_repo_exit_code))

        update_repo_exit_code = self.update_repo()
        print("Update repo: {}".format(update_repo_exit_code))

        setup_stack_exit_code = self.setup_stack_distance()
        print("Setup stack: {}".format(setup_stack_exit_code))

        status_arr = [cmake_install_exit_code, clone_repo_exit_code, update_repo_exit_code, setup_stack_exit_code]
        return all([status == 0 for status in status_arr])