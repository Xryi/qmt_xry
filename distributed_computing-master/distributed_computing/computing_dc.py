import logging
import paramiko
import os
import socket
import copy
import time
import random
from tools.parallel_computing import MultiThread, MultiProcess
from distributed_computing.computing_nodes import ComputingNodes
from base.config import REDIS_PASSWD


class ComputingDC():
    def __init__(self, node_params, open_dc=True,host_port=None):
        self.open_dc = open_dc
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.open_dc = open_dc
        self.node_params = node_params
        self.port_redis = self.get_usable_port() if host_port is None else host_port
        # self.port_redis_shared = self.usable_port[0]
        # self.port_redis = self.usable_port[1]
        self.ip = None
        self.service_nodes = None

        self.cmd_close_service = "ray stop"
        self.cmd_get_cpu_resource = 'cat /proc/cpuinfo| grep "processor"| wc -l'

    def __enter__(self):
        host_index=0
        for i in range(len(self.node_params)):
            if self.node_params[i].get('host_flag',False):
                host_index=i
        self.node_params[host_index]['host_flag']=True
        main_param=self.node_params.pop(host_index)
        self.node_params.insert(0,main_param)
        self.ip=main_param.get('server', {}).get('hostname', '')

        for param_i in self.node_params:
            param_i['host_ip'] = self.ip
            param_i['host_port'] = self.port_redis
            param_i['open_dc'] = self.open_dc

        # 1.Activate Host Service
        # 2.Activate Node Service
        self.service_nodes = self.initialize_service_nodes()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 1.Close Server Nodes
        self.logger.info("Closing DC Servers")
        self.exec_cmd_service_nodes(self.cmd_close_service)

    def initialize_service_nodes(self):
        params = copy.deepcopy(self.node_params)
        result = MultiThread.multi_thread(ComputingNodes, params)
        return result

    def exec_cmd_service_nodes(self, cmd):
        params = [{'server': server_i, 'cmd': cmd} for server_i in self.service_nodes]
        result = MultiThread.multi_thread(ComputingNodes.exec, params)
        return result

    @staticmethod
    def exec_cmd(cmd):
        result = os.popen(cmd).read()
        return result

    def get_usable_port(self,  range=(6000,60000)):
        rand_num=random.randint(*range)
        return rand_num

    @staticmethod
    def get_local_ip():
        ip = socket.gethostbyname(socket.gethostname())
        if ip == '127.0.0.1':
            ip = ""
            all_ip = socket.getaddrinfo(socket.gethostname(), None)
            for ip_i in all_ip:
                if '192.168.' in ip_i[4][0]:
                    ip = ip_i[4][0]
        return ip
