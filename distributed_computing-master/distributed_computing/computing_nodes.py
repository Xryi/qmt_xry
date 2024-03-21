import logging

import paramiko
import time
from base.config import *


class ComputingNodes():
    def __init__(self, kwargs):
        # 1.params
        self.host_flag = kwargs.get('host_flag',False)
        self.ip = kwargs.get('server', {}).get('hostname', '')
        self.server = kwargs.get("server")
        self.flag_build_env = kwargs.get("build_env", False)
        self.flag_install_reqs = kwargs.get("install_reqs", False)
        self.host_ip = kwargs.get('host_ip', "")
        self.host_port = kwargs.get('host_port', "")
        self.num_cpu = kwargs.get('num_cpu', "")
        self.open_dc = kwargs.get('open_dc', False)

        self.cmd_activate_node_service = "ray start --address={}:{} --num-cpus={} --redis-password='{}'"
        self.cmd_activate_host_service = "ray start --head --redis-port={} --num-cpus={} --redis-password='{}'"
        self.cmd_close_service = "ray stop"

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        self.req_path = REQUIREMENTS_PATH
        self.my_req_path = MY_REQUIREMENTS_PATH
        self.build_path = BUILD_PATH

        self.ssh = None

        self.run()

    @staticmethod
    def load_file2list(file_path):
        with open(file_path) as f:
            content = f.read()
        return [item for item in content.split('\n') if item]

    def connect_server(self, server):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(**server)
            if (ssh.get_transport().is_active()):
                logging.info(f"{self.ip} -> Connection Success: {self.server}")
                return ssh
            else:
                logging.error(f"{self.ip} -> Connection Failure: {self.server}")
                return None
        except Exception as err:
            logging.error(f"{self.ip} -> Connection Failure: {self.server}")
            return None

    def exec_build_environment(self):
        cmd_envs = self.load_file2list(self.build_path)
        for cmd_i in cmd_envs:
            stdin, stdout, stderr = self.ssh.exec_command(cmd_i)
            result = ''.join(stdout.readlines())
            logging.info(f"{self.ip} -> ")
            logging.info(result)

    def exec_install_reqs(self):
        cmd_reqs = self.load_file2list(self.my_req_path)
        cmd_reqs = [f'pip install {reqi}' for reqi in cmd_reqs if reqi]
        for cmd_i in cmd_reqs:
            stdin, stdout, stderr = self.ssh.exec_command(cmd_i)
            result = ''.join(stdout.readlines())
            logging.info(f"{self.ip} -> ")
            logging.info(result)

    def exec_cmd(self, cmd):
        connection_alive = False
        if self.ssh.get_transport() is not None:
            connection_alive = self.ssh.get_transport().is_active()
            self.ssh = self.connect_server(self.server)
        if connection_alive and self.ssh:
            self.logger.info(f"{self.ip} -> Exec Cmd -> {cmd}")
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
            result = ''.join(stdout.readlines())
            logging.info(f"{self.ip} -> ")
            logging.info(result)
            return [item for item in result.split('\n') if item]
        else:
            return []

    def run(self):
        self.ssh = self.connect_server(self.server)
        # 1.Base Env
        # 1.1.ENV
        if self.flag_build_env:
            self.exec_build_environment()
        # 1.2.REQS
        if self.flag_install_reqs:
            self.exec_install_reqs()

        # 2.Open Service
        if self.open_dc:
            if (self.host_flag):
                cmd_activate_node_service = self.cmd_activate_host_service.format(
                    self.host_port, self.num_cpu,REDIS_PASSWD)
                print(f"{self.ip} -> Host Activated! ->  {cmd_activate_node_service}")
            else:
                time.sleep(2)
                cmd_activate_node_service = self.cmd_activate_node_service.format(
                    self.host_ip, self.host_port,self.num_cpu, REDIS_PASSWD)
                print(f"{self.ip} -> Node Activated! ->  {cmd_activate_node_service}")

            result = self.ssh.exec_command(cmd_activate_node_service)

            # return result

    @staticmethod
    def exec(kwargs):
        server = kwargs.get('server', None)
        cmd = kwargs.get('cmd', "")
        connection_alive = False
        if server.ssh.get_transport() is not None:
            connection_alive = server.ssh.get_transport().is_active()
            server.ssh = server.connect_server(server.server)
        if connection_alive and server.ssh:
            server.logger.info(f"{server.ip} -> Exec Cmd -> {cmd}")
            stdin, stdout, stderr = server.ssh.exec_command(cmd)
            result = ''.join(stdout.readlines())
            logging.info(f"{server.ip} -> ")
            logging.info(result)
            return [item for item in result.split('\n') if item]
        else:
            return None
