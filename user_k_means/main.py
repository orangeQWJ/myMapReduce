from lib.local import start
import matplotlib.pyplot as plt

huge_file_path = "/Users/orange/code/project/MapReduce/user_k_means/huge_data.txt"
user_func_file_path = "/Users/orange/code/project/MapReduce/user_k_means/user.py"


new_center = {}

def plot_points(points_list):
    colors = ["r", "b", "g", "y", "pink", "purple", "gray", "black"]
    for i, points in enumerate(points_list):
        x = [point[0] for point in points]
        y = [point[1] for point in points]
        plt.scatter(x, y, color=colors[i % len(colors)])
    plt.show()

def handle_mid_file():
    # 提取新的簇心
    mid_file = open("./result.txt")
    global new_center
    new_center = {}
    for line in mid_file:
        if not line:
            continue
        try:
            temp = line.split("*")
            center = eval(temp[0])
            point = eval(temp[1])
        except:
            pass
        else:
            if center not in new_center:
                new_center[center] = [point]
            else:
                new_center[center].append(point)

    mid_file.close()

    keys = new_center.keys()
    keys = list(keys)
    keys.sort()
    for key in keys:
        print(key, " => ", len(new_center[key]))

    new_center_list = list(new_center.keys())
    #print(sorted(new_center))
    # 创建新的huge_data.txt,为新一轮mapreduce做准备
    # new_huge_file = open("huge_data.txt(new)", "w")
    new_huge_file = open("huge_data.txt", "w")
    mid_file = open("./result.txt")
    for line in mid_file:
        if len(line.strip()) == 0:
            continue
        print(line.strip() + "*" + str(new_center_list), file=new_huge_file)
    return new_center



points_list = []
for i in range(5):
    print("===========%d==============" % (i + 1))
    start(huge_file_path, user_func_file_path)
    handle_mid_file()
    points_list.append(list(new_center.keys()))
    for v in new_center.values():
        points_list.append(v)
    # ❌
    #plot_points(points_list)
    # ❌
    points_list = []



keys = new_center.keys()
keys = list(keys)
keys.sort()
for key in keys:
    print((round(key[0]), round(key[1])), " => ", len(new_center[key]))

new_center_list = list(new_center.keys())
