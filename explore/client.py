import socket
import pickle
tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_client_socket.connect(("10.211.55.4", 3000))
#tcp_client_socket.connect(("localhost", 3000))
#s = "1" * 1000 * 1000 * 1000
s = "1" * 7 * 1000 * 10  * 10 * 10
#tcp_client_socket.sendall(pickle.dumps(s))
tcp_client_socket.sendall(s.encode())
