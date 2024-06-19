import socket
import queue
from threading import Thread
from time import sleep
import pickle
import config

from xmlrpc.server import SimpleXMLRPCServer

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
#master_ip = '10.211.55.4'
## ports
#task_submission_port = 8848
#worker_connection_port = 60014
#master_submit_port = 60026
##task_end_port = 60010
#decide_M_R_port = 60098

# 临时存放用户发布总任务
user_sub_task_Queue = queue.Queue()
# 存放总任务拆分后的的子任务
sub_task_Queue = queue.Queue()
# 空闲worker队列,(套接字,(ip,port))
free_worker_Queue = queue.Queue()

# 存放多个总任务执行状况的字典
tasks_status = dict()

# ing_worker_Queue = queue.Queue()


# tcp接受数据时,一次性接受所有的数据
def recvall(sock, buffer_size=1024):
    data = b""
    while True:
        part = sock.recv(buffer_size)
        data += part
        if len(part) < buffer_size:
            break
    return data

# master决定将次任务分割多少个M子任务,多少个R子任务


def decide_M_R(task_message):
    size = task_message["file_size"]
    segmentation_proposal = {
        "count_of_M": 4+size*0,
        "count_of_R": 2+size*0,
    }
    return segmentation_proposal


# 决定总任务分割成多少个M任务,多少R任务
class D(Thread):
    def run(self):
        server_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_s.bind((config.master_ip, config.decide_M_R_port))
        server_s.listen(128)
        while True:
            # new_s, client_info = server_s.accept()
            new_s, _ = server_s.accept()
            # task_message = pickle.loads(new_s.recv(1024*1024*1024))
            task_message = pickle.loads(recvall(new_s))
            segmentation_proposal = decide_M_R(task_message)
            new_s.sendall(pickle.dumps(segmentation_proposal))


d = D()
d.start()

# 专门接待提交用户任务的线程


class A(Thread):
    def run(self):
        server_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_s.bind((config.master_ip, config.task_submission_port))
        server_s.listen(128)
        while True:
            new_s, client_info = server_s.accept()
            # task_message = pickle.loads(new_s.recv(1024*1024*1024))
            task_message = pickle.loads(recvall(new_s))
            # 因为NAT会进行网络转换
            task_message["ip"] = client_info[0]
            task_message["socket"] = new_s
            receive_task_log = """
🌈 🌈 🌈 🌈 🌈 🌈 🌈 🌈 🌈 🌈 🌈 
收到 <%(ip)s>:<%(pid)s> 提交的任务请求
huge文件路径: %(ip)s:%(file_path)s
文件大小:%(file_size)s B.
已在本地被分割成%(count_of_M)s块
子文件放在目录: %(file_dir)s
%(transport_port)s 端口已开启文件传输服务
用户定义的map,reduce函数位于: %(map_reduce_func_path)s """ % task_message
            # master接受任务的相关信息
            print(receive_task_log)
            user_sub_task_Queue.put(task_message)
            # new_s.close()
            # 不关闭是为了继续与local进程交流
            # 回传最终的reduce结果


a = A()
a.start()


# 从用户发布的总任务队列中接取任务,生成总任务相关数据结构
class B(Thread):
    def run(self):
        while True:
            # 从用户发布的任务中取出一个任务
            # 不放回
            task = user_sub_task_Queue.get()

            # 使用进程ip,pid 标识一次总任务
            # (ip, pid) (字符,数字)
            task_key = (task['ip'], task["pid"])

            tasks_status[task_key] = {
                # m,r 子任务的执行情况
                "m_status": [],
                "r_status": [],
                # 存放m任务产生的m*r个中间文件的位置
                'mid_files_path': [[None]*task["count_of_R"] for _ in range(task["count_of_M"])],
                # 记录总任务的执行阶段
                "status": "mapping",  # reducing/done
                # 当所有map子任务都完成后,
                # mapping -> reducing,开始分发reduc子任务
                # 当所有reduce子任务都完成
                # reducing -> done,任务完成后的收尾工作
                "map_reduce_func_path": (task['ip'], task['transport_port'],  task['map_reduce_func_path']),
                "count_of_M": task["count_of_M"],
                "count_of_R": task["count_of_R"],
                # 提交任务的socket
                # 保留,用来回送reduce任务结果
                "socket": task['socket']
            }
            # 每个M任务的处理状态,每个M任务对应的待分析文件的位置
            for sub_file in task["sub_file_list"]:
                tasks_status[task_key]["m_status"].append(
                    {
                        # 待处理/已派单/处理中/已完成
                        "status": "待处理",
                        # 总任务被拆分为多个m子任务时
                        # 待处理 -> 已派单

                        # 子任务分配给某个worker时
                        # 已派单 > 处理中

                        # worker 完成任务
                        # 处理中 > 已完成

                        # huge文件的切片文件的路径
                        # 文件由(ip,传输服务端口,绝对路径) 唯一标识
                        # (str, int, str)
                        "sub_file_path": (task["ip"], task["transport_port"], sub_file),
                        # 正在处理当前m子任务的进程
                        "worker": None,  # (套接字,(ip,port))
                    }
                )
            for _ in range(task["count_of_R"]):
                tasks_status[task_key]["r_status"].append(
                    {
                        # 未准备好/待处理/已派单/处理中/已完成
                        "status": "未准备好",
                        # 所有m子任务都完成时
                        # 未准备好 -> 待处理

                        # R任务被拆分为多个r子任务时
                        # 待处理 -> 已派单

                        # 子任务分配给某个worker时
                        # 已派单 -> 处理中

                        # worker 完成任务
                        # 处理中 > 已完成

                        # 因为reduce产生的文件比较小,直接将内容放下master中
                        "reduce_out_file_content": None,
                        # 正在处理当前r子任务的进程
                        "worker": None,
                    }
                )


b = B()
b.start()

# 接待连入的worker,算力增加


class W(Thread):
    def run(self):
        server_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_s.bind((config.master_ip, config.worker_connection_port))
        server_s.listen(128)
        while True:
            new_s, client_info = server_s.accept()
            worker_online_log = """
🤖 🤖 🤖 🤖 🤖 🤖
    worker: {0} 上线 """.format(client_info)
            print(worker_online_log)
            free_worker_Queue.put((new_s, client_info))


w = W()
w.start()


# 将总任务拆分成多个子任务
class P(Thread):
    def run(self):
        while True:
            sleep(1)
            # 派单map子任务
            for task_key in tasks_status:
                # 从若干总任务中取出一个
                task = tasks_status[task_key]
                for i, sub_m_task in enumerate(task['m_status']):
                    if sub_m_task['status'] == "待处理":
                        sigle_task_dict = {
                            "task_type": 'map',
                            "big_task_id": task_key,
                            "m_task_i": i,
                            # m文件切片的地址是三元组
                            "m_file_path": sub_m_task['sub_file_path'],
                            'map_reduce_func_path': task["map_reduce_func_path"],
                            'count_of_R': task['count_of_R']
                        }
                        sub_m_task['status'] = "已派单"
                        sub_task_Queue.put(sigle_task_dict)
                        deliver_subtask_log = """
🎯 🎯 🎯 🎯 🎯
    {0} 总任务的 {1}号子{2}任务已派单,等待处理 """.format(task_key, i, 'map')
                        print(deliver_subtask_log)
            # 派单reduce子任务
            # ❗️
            # ❗️
            # ❗️
            # ❗️
            for task_key in tasks_status:
                task = tasks_status[task_key]
                for i, sub_r_task in enumerate(task['r_status']):
                    if sub_r_task['status'] == "待处理":
                        sigle_task_dict = {
                            "task_type": 'reduce',
                            "big_task_id": task_key,
                            "r_task_i": i,
                            "r_file_path": [x[i] for x in task['mid_files_path']],
                            'map_reduce_func_path': task["map_reduce_func_path"],
                            'count_of_M': task['count_of_M']
                        }
                        sub_r_task['status'] = "已派单"
                        # print("判罚 reduce子任务", i)
                        sub_task_Queue.put(sigle_task_dict)
                        deliver_subtask_log = """
🎯 🎯 🎯 🎯 🎯
    {0} 总任务的 {1}号子{2}任务已派单,等待处理 """.format(task_key, i, 'reduce')
                        print(deliver_subtask_log)


p = P()
p.start()
# 找一个子任务,然后将他分配给一个worker


class O(Thread):
    def run(self):
        while True:
            # 取出一个子任务
            sub_task_dict = sub_task_Queue.get()
            # 取出一个空闲worker,  (socket, (ip,port))
            worker_message = free_worker_Queue.get()
            worker_soket = worker_message[0]
            # 需要判断这个worker是否还在线
            # ❗️
            # ❗️
            # ❗️
            # 将子任务字典通过网络发给worker
            try:
                worker_soket.sendall(pickle.dumps(sub_task_dict))
            except:
                sub_task_Queue.put(sub_task_dict)
                print("""
                ❗️❗️❗️❗️❗️❗️
                worker: {0} 
                在为它分配任务时发现其离线
                当前worker并没有任务,需要管理员手动重新连接
                """.format(worker_message))
                continue
            # 如果是已分配了任务的worker离线改如何处理
            # 还有什么问题需要考虑????
            # 这里需要针对任务类型分别讨论
            # ❗️
            # ❗️
            # ❗️
            # 任务已下发
            # 看一下子任务属于哪个总任务
            # 修改子任务状态
            if sub_task_dict['task_type'] == 'map':
                big_task_key = sub_task_dict["big_task_id"]
                m_i = sub_task_dict['m_task_i']
                big_task = tasks_status[big_task_key]
                big_task['m_status'][m_i]['status'] = "处理中"
                # (套接字,(ip,port))
                big_task['m_status'][m_i]['worker'] = worker_message

                issue_subtask_log = """
🏋️ 🏋️ 🏋️ 🏋️ 🏋️ 🏋️
    {0} 总任务的 {1}号子{2}任务已分配worker
    worker 信息{3} """.format(
                    big_task_key,
                    m_i,
                    sub_task_dict['task_type'],
                    worker_message[1],
                )
                print(issue_subtask_log)
            elif sub_task_dict['task_type'] == 'reduce':
                big_task_key = sub_task_dict["big_task_id"]
                r_i = sub_task_dict['r_task_i']
                big_task = tasks_status[big_task_key]
                big_task['r_status'][r_i]['status'] = "处理中"
                # (套接字,(ip,port))
                big_task['r_status'][r_i]['worker'] = worker_message
                issue_subtask_log = """
🏋️ 🏋️ 🏋️ 🏋️ 🏋️ 🏋️
    {0} 总任务的 {1}号子{2}任务已分配worker
    worker 信息{3} """.format(
                    big_task_key,
                    r_i,
                    sub_task_dict['task_type'],
                    worker_message[1],
                )
                print(issue_subtask_log)


o = O()
o.start()
# 接受worker的工作汇报


class S(Thread):
    def run(self):
        server_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_s.bind((config.master_ip, config.master_submit_port))
        server_s.listen(128)
        while True:
            new_s, client_info = server_s.accept()
            #result_of_task = pickle.loads(new_s.recv(1024*1024*1024))
            result_of_task = pickle.loads(recvall(new_s))
            if result_of_task['task_type'] == 'map':
                task = tasks_status[result_of_task['big_task_id']]

                i_index = result_of_task['m_task_i']
                for i in range(task['count_of_R']):
                    j_index = i
                    # print("i,j_index",i_index,j_index)
                    # ❌
                    # ❌
                    # ❌
                    # 这里应该从worker的端口读 中间文件
                    # 已修复 待测试
                    task['mid_files_path'][i_index][j_index] = (
                        client_info[0], result_of_task['transport_port'], result_of_task['mid_file_list'][i])
                    # ❌
                    # map子任务产生的文件数可能少于 count_of_R
                    # ❌
                    # ❌
                want_job_message = task["m_status"][result_of_task['m_task_i']]["worker"]
                free_worker_Queue.put(want_job_message)
                task['m_status'][i_index]['status'] = "已完成"

                finish_subtask_log = """
🫡 🫡 🫡 🫡 🫡 🫡
    {0} 总任务的 {1}号子{2}任务已完成 """.format(
                    result_of_task['big_task_id'],
                    i_index,
                    'map'
                )
                print(finish_subtask_log)

                # 计算已完成多少m子任务
                count_of_finished_m_task = 0
                for i in range(task['count_of_M']):
                    if task['m_status'][i]['status'] == '已完成':
                        count_of_finished_m_task += 1
                # 若m子任务全部完成,则可以开始准备处理r子任务了
                # 所有的r子任务都可以进入待处理状态
                if count_of_finished_m_task == task['count_of_M']:
                    task['status'] = "reducing"
                    for i in range(task['count_of_R']):
                        task['r_status'][i]['status'] = "待处理"
            elif result_of_task['task_type'] == 'reduce':
                task = tasks_status[result_of_task['big_task_id']]
                i_index = result_of_task['r_task_i']
                task['r_status'][i_index]['reduce_out_file_content'] = result_of_task['reduce_out_file_content']
                task['r_status'][i_index]['status'] = "已完成"
                want_job_message = task["r_status"][result_of_task['r_task_i']]["worker"]
                free_worker_Queue.put(want_job_message)

                finish_subtask_log = """
🫡 🫡 🫡 🫡 🫡 🫡
    {0} 总任务的 {1}号子{2}任务已完成 """.format(
                    result_of_task['big_task_id'],
                    i_index,
                    'reduce'
                )
                print(finish_subtask_log)

                # 计算已完成多少r子任务
                count_of_finished_r_task = 0
                for i in range(task['count_of_R']):
                    if task['r_status'][i]['status'] == '已完成':
                        count_of_finished_r_task += 1
                # 若r子任务全部完成,则总任务已经完成
                if count_of_finished_r_task == task['count_of_R']:
                    task['status'] = 'done'



s = S()
s.start()

# 告知用户任务结束


class E(Thread):
    def run(self):
        while True:
            key_of_finish_task = []
            for task_key in tasks_status:
                # 从若干总任务中取出一个
                task = tasks_status[task_key]
                if task["status"] == 'done':
                    print("""
🤖 🤖 🤖 🤖 🤖
    任务{0}
    已完成
    将reduce任务产生的文件回传给local """.format(task_key))
                    task['status'] = "done"
                    key_of_finish_task.append(task_key)
                    reduce_out_all = ""
                    for _, r_task in enumerate(task['r_status']):
                        # print(r_task['reduce_out_file_content'])
                        reduce_out_all += r_task['reduce_out_file_content']
                        # print("""
                        # {0}
                        # {1}
                        # """.format(i, r_task['reduce_out_file_content']))
                    # 向最开始提交任务的套接字会写
                    task['socket'].sendall(reduce_out_all.encode("utf-8"))
            for key in key_of_finish_task:
                def remote_del_file(ip, port, path):
                    tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    tcp_client_socket.connect((ip, port))
                    # 告诉对方我想要删除哪个文件
                    tcp_client_socket.send(("del: " + path).encode("utf-8"))
                for x in tasks_status[key]["mid_files_path"]:
                    for y in x:
                        remote_del_file(*y)

                del tasks_status[key]
                print(key, "任务已删除")
                print("length of tasks_status", len(tasks_status))
            sleep(1)


e = E()
e.start()

while True:
    print("=========================================")
    cmd = input('> ')
    # if cmd == "no":
    #    continue
    # eval(cmd)

    print("user_sub_task_Queue", user_sub_task_Queue.empty())
    print("sub_task_Queue", sub_task_Queue.empty())
    print("free_worker_Queue", free_worker_Queue.empty())
    print("free_worker_Queue.get() 执行之后")
    x = free_worker_Queue.get()
    print("free_worker_Queue", free_worker_Queue.empty())

    if cmd == "reset":
        user_sub_task_Queue = queue.Queue()
        sub_task_Queue = queue.Queue()
        free_worker_Queue = queue.Queue()
        print("master 重启")

    print("=========================================")
