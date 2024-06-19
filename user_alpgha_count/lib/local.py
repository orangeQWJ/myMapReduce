import os
import sys
import stat
import socket
import pickle
from lib.tool import *
from user import *
import config
# from config import *
import subprocess


import xmlrpc.client
import config
# proxy = xmlrpc.client.ServerProxy("http://10.211.55.4:8000/")
# rpc_server_address = "http://%s:%s/" % (config.master_ip, config.rpc_port)
# proxy = xmlrpc.client.ServerProxy(rpc_server_address)

# 待分割文件的绝对路径
# huge_file_path = sys.argv[1]
# user_func_file_path = sys.argv[2]
# master_ip = sys.argv[3]

huge_file_path = "/Users/orange/code/project/MapReduce/user/huge_file"
# huge_file_path_dir = get_file_dir(huge_file_path)
huge_file_path_dir = os.path.dirname(huge_file_path)
# 用户自定义的map,reduce函数所在文件的绝对路径
user_func_file_path = "/Users/orange/code/project/MapReduce/user/user.py"

# 最终所有reduce文件合并后size的上限
# 1MB
# max_size_of_all_reduce_file = 1024*1024


# mast所运行的ip,端口
#master_ip = "10.211.55.4"
master_ip = "127.0.0.1"
# 提交任务使用的端口
task_submission_port = 8848
# 文件传输服务所运行的端口
transport_port = 60000
# 协商分割方案
decide_M_R_port = 60098

# 分割产生的子文件的前缀
prefix = "hcbhxcvaniul_"

# 先对本地文件分割


def split(n):
    # map分割的文件产生在执行路径
    #######
    output = subprocess.check_output(['wc', "-l", huge_file_path])
    num_lines = int(output.split()[0])
    num_lines_of_each_subfile = num_lines // n
    #######
    #split_cmd = "split -n {0} -a 3 -d {1} {2}".format(
    split_cmd = "split -l {0} -a 3 -d {1} {2}".format(
        num_lines_of_each_subfile, huge_file_path, prefix)

    os.system(split_cmd)
    print("huge_file 分割完毕")
    # for x in os.listdir(huge_file_path_dir):
    #    if  x.startswith(prefix):
    #        print(x)

# 善后,删除临时产生的分片文件
# 具体删除dir文件夹下以prefix作为文件名前缀的文件


def del_splited_file():
    current_path = os.getcwd()
    # os.system("rm {0}/{1}*".format(huge_file_path_dir, prefix))
    os.system("rm {0}/{1}*".format(current_path, prefix))
    print("huge_file 分片文件删除完毕")

# 获取文件大小


def file_size(file_path):
    myfile_stat = os.stat(file_path)
    size = myfile_stat[stat.ST_SIZE]
    return size

# tcp接受数据时,一次性接受所有的数据


def recvall(sock, buffer_size=1024):
    data = b""
    while True:
        part = sock.recv(buffer_size)
        data += part
        if len(part) < buffer_size:
            break
    return data

# 上传任务信息,master决定将次任务分割多少个M子任务,多少个R子任务


def up_task_feature():
    client_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_s.connect((master_ip, decide_M_R_port))
    task_message = {
        "file_size": file_size(huge_file_path),  # huge文件大小
    }
    client_s.sendall(pickle.dumps(task_message))
    segmentation_proposal = pickle.loads(client_s.recv(1024))
    return segmentation_proposal

# 将自己的文件信息上传至master


def up_task_message(segmentation_proposal):

    client_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_s.connect((master_ip, task_submission_port))

    temp_splited_files = list(os.listdir(huge_file_path_dir))

    # 存放所有分割得到子文件的文件名
    splited_files = []
    for x in temp_splited_files:
        if x.startswith(prefix):
            splited_files.append(huge_file_path_dir + "/" + x)

    task_message = {
        "ip": "None",
        "pid": os.getpid(),  # 提交任务的进程的id
        "file_dir": huge_file_path_dir,  # huge文件所在目录的绝对路径
        "file_path": huge_file_path,  # huge 文件绝对路径
        "file_size": file_size(huge_file_path),  # huge文件大小
        "sub_file_list": splited_files,  # 切割产生的子文件的绝对路径
        "transport_port": transport_port,  # 文件传输服务使用的端口号
        "map_reduce_func_path": user_func_file_path,  # 用户定义的map,reduce函数的源文件绝对地址
        "count_of_M": segmentation_proposal['count_of_M'],
        "count_of_R": segmentation_proposal['count_of_R'],
    }
    # 将任务发布给master
    client_s.sendall(pickle.dumps(task_message))
    # 等待最终结果,得到所有reduce任务结果的汇集
    # reduce_out_all = client_s.recv(1024*1024).decode("utf-8")
    reduce_out_all = recvall(client_s).decode("utf-8")
    f = open("result.txt", "w")
    print(reduce_out_all, file=f)
    client_s.close()


def start(p_huge_file_path, p_user_func_file_path):
    # 首先拿到切割方案,在程序运行无误后,改成rpc通信
    global huge_file_path
    global user_func_file_path
    global huge_file_path_dir
    huge_file_path = p_huge_file_path
    user_func_file_path = p_user_func_file_path
    huge_file_path_dir = os.path.dirname(huge_file_path)

    segmentation_proposal = up_task_feature()
    split(segmentation_proposal['count_of_M'])
    up_task_message(segmentation_proposal)
    del_splited_file()
