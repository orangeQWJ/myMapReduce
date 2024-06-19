import socket
import pickle

def recvall(sock, buffer_size=10000):
    data = b""
    while True:
        part = sock.recv(buffer_size)
        print(len(data), len(part))
        data += part
        if len(part) < buffer_size:
            print("!!!!!!!", len(part))
            break
    return data

server_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#server_s.bind(("localhost", 3000))
server_s.bind(("10.211.55.4", 3000))
server_s.listen(10)
while True:
    new_s, client_info = server_s.accept()
    #data = pickle.loads(recvall(new_s))
    data = recvall(new_s)
    print(len(data))
