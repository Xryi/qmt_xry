import socket
import ray
import time


@ray.remote()
class TaskTest():
    def __init__(self):
        self.n = 0

    def read(self):
        time.sleep(1)
        self.n+=1
        print(f" {self.get_local_ip()}  -> {self.n}")
        return ray.services.get_node_ip_address()


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