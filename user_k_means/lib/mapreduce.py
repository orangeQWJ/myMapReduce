import hashlib
# 用来存放中间(key,values)
intermediate_key_values = []
cache_dict = {}


def myhash(value, N):
    # 内置hash每次输出不确定
    # 添加字典缓存,提高性能
    global cache_dict
    if value in cache_dict:
        return cache_dict[value]
    my_string = value
    hash_object = hashlib.sha256()
    hash_object.update(my_string.encode())
    hash_str = hash_object.hexdigest()
    temp = int(hash_str, 16) % N
    cache_dict[value] = temp
    return temp


def EmitIntermediate(key, value):
    intermediate_key_values.append((key, value))


def save2file(file_name, List):
    # 将List中的内容存入文件<file_name>中
    # 以如下格式
    # key: vlaue
    f = open(file_name, "w")
    for x in List:
        print(x[0], ": ", x[1], file=f)
    f.close()


def handle_intermediate_key_values(prefix, map_id, count_of_R):
    # 组织用户map提取出的(key,value)
    # 暂存经过分区函数按照key分离的(key,value)
    global intermediate_key_values
    lists = [[].copy() for _ in range(count_of_R)]

    for key, value in intermediate_key_values:
        lists[myhash(key, count_of_R)].append((key, value))
    for i, l in enumerate(lists):
        l.sort()
        # map任务产生的中间文件的命名
        # mapid-reduceid
        save2file(prefix + str(map_id)+"-"+str(i)+".txt", l)
    intermediate_key_values = []
