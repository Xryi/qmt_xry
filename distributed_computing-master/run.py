import logging
import time

import ray

from distributed_computing.computing_dc import ComputingDC


# from tasks.task_test import TaskTest

@ray.remote
def func():
    time.sleep(1)
    return ray.services.get_node_ip_address()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    node_params = [
        {
            'server': {'hostname': '192.168.31.43', 'port': 22, 'username': 'xieruiyi', 'password': 'xry123'},
            'num_cpu': 80,
            'build_env': False,
            'install_reqs': True,
        },
        # {
        #     'server': {'hostname': '192.168.31.124', 'port': 22, 'username': 'xxx', 'password': 'xxx'},
        #     'num_cpu': 80,
        #     'build_env': False,
        #     'install_reqs': False,
        # },
        # {
        #     'server': {'hostname': '192.168.31.234', 'port': 22, 'username': 'xxx', 'password': 'xxx'},
        #     'num_cpu': 80,
        #     'build_env': False,
        #     'install_reqs': False,
        # },
        {
            'server': {'hostname': '192.168.31.165', 'port': 22, 'username': 'xieruiyi', 'password': 'BJqRD+k1'},
            'num_cpu': 60,
            'build_env': False,
            'install_reqs': True,
        },
        {
            'server': {'hostname': '192.168.31.110', 'port': 22, 'username': 'xieruiyi', 'password': 'Q0mxeswX'},
            'num_cpu': 60,
            'build_env': False,
            'install_reqs': True,
        },
        {
            'server': {'hostname': '192.168.31.222', 'port': 22, 'username': 'xieruiyi', 'password': '585VynMb'},
            'num_cpu': 60,
            'build_env': False,
            'install_reqs': True,
            'host_flag': True
        }
    ]

    with ComputingDC(node_params, open_dc=False, host_port=6000) as dc:
        time.sleep(10000000)
