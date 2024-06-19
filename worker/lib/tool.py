import socket
import os

def get_file_name(path):
    x = 0
    for i in range(len(path)-1,-1, -1):
        if path[i] == '/':
            x = i
            break
    file_name = path[x+1:]
    return file_name
def get_file_dir(path):
    x = 0
    for i in range(len(path)-1,-1, -1):
        if path[i] == '/':
            x = i
            break
    file_name = path[:x]
    return file_name

def save_file(conn, filename):
    with open(filename+'recviveing', 'wb') as f:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            f.write(data)
        os.system("mv {0} {1} ".format(filename+"recviveing", filename))

    

def remote_get_file(ip, port, path):
    tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_client_socket.connect((ip, port))
    # 告诉对方我想要哪个文件
    tcp_client_socket.send(path.encode("utf-8"))


    file_name = get_file_name(path)
    save_file(tcp_client_socket, file_name)

    tcp_client_socket.close()


