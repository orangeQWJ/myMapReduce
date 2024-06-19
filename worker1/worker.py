from lib.tool import *
from lib.mapreduce import *
import socket
import pickle
import os
import importlib
import user

transport_port = 60000
#master_ip = "10.211.55.4"
master_ip = "127.0.0.1"
worker_connection_port = 60014
master_submit_port = 60026

tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_client_socket.connect((master_ip, worker_connection_port))

# tcp接受数据时,一次性接受所有的数据
def recvall(sock, buffer_size=1024):
    data = b""
    while True:
        part = sock.recv(buffer_size)
        data += part
        if len(part) < buffer_size:
            break
    return data

while True:
    #sigle_task = pickle.loads(tcp_client_socket.recv(1024*1024*1024))
    sigle_task = pickle.loads(recvall(tcp_client_socket))
    if sigle_task["task_type"] == 'map':
        print("""
🏋️ 🏋️ 🏋️ 🏋️ 🏋️ 🏋️
    任务类型: {0}
    所属总任务id: {1}
    这是第{2} 个m 子任务
    所需读取文件位置: 
    {3}
    用户定义的map_reduce函数位于: {4}
    该M子任务,产生{5} 个中间文件
""".format(
                sigle_task['task_type'],
                sigle_task['big_task_id'],
                sigle_task['m_task_i'],
                sigle_task['m_file_path'],
                sigle_task['map_reduce_func_path'],
                sigle_task['count_of_R']

            ))
        remote_get_file(*(sigle_task['m_file_path']))
        # 将文件拉到本地,以原文件名保存
        file_name = get_file_name(sigle_task["m_file_path"][2])
        # 获取用户的map,reduce源代码
        remote_get_file(*(sigle_task['map_reduce_func_path']))
        importlib.reload(user)
        user.my_map(file_name)
        mid_file_prefix = sigle_task['big_task_id'][0] + str(sigle_task['big_task_id'][1])
        handle_intermediate_key_values(mid_file_prefix,sigle_task['m_task_i'],sigle_task['count_of_R'] )
        # 删除huge文件切片
        os.system("rm {0}".format(file_name))

        # 如何删除 map任务产生的中间文件
        
        mid_file_list = [
            # 当前进程的目录
            os.getcwd() + "/" +
            mid_file_prefix +
            str(sigle_task['m_task_i']) +
            '-' +
            str(i) +
            ".txt" for i in range(sigle_task['count_of_R'])]
        # ❗️
        #   确保文件已经创立
        #   要去看看用户的handle_intermediate_key_values函数,看看是不是一定产生
        #   count_of_R 个中间文件
        #   已确定
        #   一定会创建 count_of_R 个文件
        # ❗️
        result_of_task = {
            "task_type" : "map",
            "big_task_id": sigle_task['big_task_id'],
            "m_task_i": sigle_task['m_task_i'],
            "count_of_R": sigle_task['count_of_R'],
            "mid_file_list" : mid_file_list,
        # ❗️
        # ❗️worker 在完成了map任务后要尝判断本机是否已启动
        #   文件传输服务,若没启动,应该启动并确定端口
            "transport_port": transport_port,
        # ❗️
        }
        submit_result_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        submit_result_socket.connect((master_ip, master_submit_port))
        submit_result_socket.sendall(pickle.dumps(result_of_task))
        submit_result_socket.close()

    elif sigle_task['task_type'] == 'reduce':
        files_list_to_print = "\n"
        for x in sigle_task['r_file_path']:
            files_list_to_print += x[0] + " : " + x[-1] + '\n'
        print("""
🏋️ 🏋️ 🏋️ 🏋️ 🏋️ 🏋️
    任务类型: {0}
    所属总任务id: {1}
    这是第{2} 个r 子任务
    所需读取文件位置: 
    {3}
    用户定义的map_reduce函数位于: {4}
    该R子任务,合并{5} 个中间文件
""".format(
                sigle_task['task_type'],
                sigle_task['big_task_id'],
                sigle_task['r_task_i'],
                #sigle_task['r_file_path'],
                files_list_to_print,
                sigle_task['map_reduce_func_path'],
                sigle_task['count_of_M']

            ))
        # reduce子任务
        for file_path in sigle_task['r_file_path']:
            remote_get_file(*(file_path))
        file_list = [ get_file_name(f[-1])  for f in sigle_task['r_file_path']]
        # 获取map reduce 源码
        remote_get_file(*(sigle_task['map_reduce_func_path']))
        importlib.reload(user)
        
        # 对多个文件合并排序
        # 这是粗糙的实现
        # ❗️使用cat和重定位重写
        context = ''
        for x in file_list:
            f = open(x, 'r')
            f_context = f.read()
            #print(
            #    """
            #    文件名{0}
            #    内容
            #    {1}
            #    """.format(x, f_context)
            #)
            context += f_context

            f.close()
        mid_key_value = context.split('\n')
        mid_key_value = [x.split(':') for x in mid_key_value if x]
        key_value_dict = {}
        for x in mid_key_value:
            key = x[0].strip()
            value = x[1].strip()
            if key not in key_value_dict.keys():
                key_value_dict[key] = []
                key_value_dict[key].append(value)
            else:
                key_value_dict[key].append(value)

        
        for k in key_value_dict:
            user.my_reduce(k,key_value_dict[k])
        if len(key_value_dict) == 0:
            reduce_out_file_content = '   '
        else:
            reduce_out_file_content = open("reduce_out.txt", "r").read()
        result_of_task = {
            'task_type' : 'reduce',
            "big_task_id": sigle_task['big_task_id'],
            "r_task_i": sigle_task['r_task_i'],
            'reduce_out_file_content': reduce_out_file_content,
            "transport_port": transport_port,
        }
        os.system('rm reduce_out.txt')
        #for file_name in file_list:
        #    print("删除",file_name)
            #os.system("rm {0}".format(file_name))

        submit_result_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        submit_result_socket.connect((master_ip, master_submit_port))
        submit_result_socket.sendall(pickle.dumps(result_of_task))
        submit_result_socket.close()



#tcp_client_socket.close()


