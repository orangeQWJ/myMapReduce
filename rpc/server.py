from xmlrpc.server import SimpleXMLRPCServer
from threading import Thread
import config


class RPC(Thread):
    def run(self):
        # 准备待注册的rpc服务
        def re_dict(name, age):
            return {"name": name, "age": age}

        def re_sum(a, b):
            return a + b
        server = SimpleXMLRPCServer((config.master_ip, config.rpc_port))
        # 注册rpc服务
        server.register_function(re_dict, "is_even")
        server.register_function(re_sum, "sum")
        print("rpc server listening on port %d..." % config.rpc_port)
        server.serve_forever()


rpc = RPC()
rpc.start()
