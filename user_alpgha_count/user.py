# 用户根据自己的业务编写map,reduce函数
# map(file_name)
# reduce(key, valuse)

# from mapreduce import EmitIntermediate, handle_intermediate_key_values
from lib.mapreduce import EmitIntermediate


def my_map(file_name):
    # flie_name 文件名
    # 此map用于统计各个英文字母出现的个数
    # 文件可能很大,所以使用迭代的方式读入
    for line in open(file_name):
        data = line.split("*")
        current_center = eval(data[0])
        point = eval(data[1])
        center_list = eval(data[2])
        closest_center = current_center
        min_distance = (point[0] - closest_center[0]) ** 2 + \
            (point[1] - closest_center[1]) ** 2
        for center in center_list:
            distance = (point[0] - center[0]) ** 2 + \
                (point[1] - center[1]) ** 2
            if distance < min_distance:
                closest_center = center
        # line_format = "%s*%s*%s"
        mid_key = str(closest_center)
        mid_value = "%s*%s" % (point, center_list)
        EmitIntermediate(mid_key, mid_value)


def my_reduce(key, values):
    # my_reduce所有的操作都不是在你的本机上执行的
    # 将你想得到的数据全部写入文件
    # reduce_out.txt
    # ❗️
    _ = key
    f = open("reduce_out.txt", "a")
    points = [eval(point.split("*")[0]) for point in values]
    x_sum = sum([point[0] for point in points])
    y_sum = sum([point[1] for point in points])
    new_center = (x_sum/len(points), y_sum/len(points))
    #print(key, "=> new_center", new_center, file=f)
    for point in points:
        print(new_center, "*", point, file=f)
    # ❗️


# def handle_intermediate_key_values(prefix, map_id, count_of_R):
# if __name__ == "__main__":
    # my_map("./huge_file")
    # handle_intermediate_key_values("xxxx", 1, 2)
