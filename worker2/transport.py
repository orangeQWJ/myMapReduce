import socket
from threading import Thread
import os

transport_port = 60000

# 定义传输函数
def send_file(conn, filename):
    with open(filename, "rb") as f:
        while True:
            data = f.read(1024)
            if not data:
                break
            conn.send(data)
# 在本机的transport_port 端口启动一个进程,用于远程传输文件
def transport_file():
    def run(socket_to_cli, client_info):
        path_to_read = socket_to_cli.recv(1024).decode("utf-8")
        if path_to_read.startswith("del: "):
            path_to_del = path_to_read.split()[1]
            print(path_to_del)
            print("{0} 请求从本机删除 {1}".format(client_info, path_to_del))
            os.system("rm {0}".format(path_to_del))
            print("已删除 %s" % path_to_del)
        else:
            print("{0} 请求从本机获取 {1}".format(client_info, path_to_read))
            send_file(socket_to_cli, path_to_read)
            print("已向{0} 发送 {1}".format(client_info, path_to_read))
        socket_to_cli.close()

    server_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #server_s.bind(("10.211.55.4", 60000))
    server_s.bind(("", transport_port))
    server_s.listen(128)
    while True:
        new_s, client_info = server_s.accept()
        T1 = Thread(target=run,args=(new_s,client_info)) 
        T1.start()
if __name__ == "__main__":
    transport_file()
