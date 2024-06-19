# 用户根据自己的业务编写map,reduce函数
# map(file_name)
# reduce(key, valuse)

#from mapreduce import EmitIntermediate, handle_intermediate_key_values
from lib.mapreduce import EmitIntermediate


def my_map(file_name):
    # flie_name 文件名
    # 此map用于统计各个英文字母出现的个数
    # 文件可能很大,所以使用迭代的方式读入
    for line in open(file_name):
        for alpha in line:
            if alpha.isalpha():
                EmitIntermediate(alpha, 1)


def my_reduce(key, values):
    # my_reduce所有的操作都不是在你的本机上执行的
    # 将你想得到的数据全部写入文件
    # reduce_out.txt
    # ❗️
    f = open("reduce_out.txt", "a")
    print(key, ": ", len(values), file=f)
    # ❗️


# def handle_intermediate_key_values(prefix, map_id, count_of_R):
#if __name__ == "__main__":
    #my_map("./huge_file")
    #handle_intermediate_key_values("xxxx", 1, 2)
