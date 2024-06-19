import matplotlib.pyplot as plt
import random
import math


def generate_points(n, a, b, r):
    points = []
    for _ in range(n):
        x = random.uniform(a - r, a + r)
        y_range = math.sqrt(r ** 2 - (x - a) ** 2)
        y = random.uniform(b - y_range, b + y_range)
        points.append((x, y))
    return points


def plot_points(points_list):
    colors = ["r", "b", "g", "y", "pink", "purple", "gray", "black"]
    for i, points in enumerate(points_list):
        x = [point[0] for point in points]
        y = [point[1] for point in points]
        plt.scatter(x, y, color=colors[i % len(colors)])
    plt.show()


points_list = [
    # 7 个簇中心
    generate_points(10000, 0, 0, 1),
    generate_points(10000, 3, 5, 2),
    generate_points(10000, 5, 20, 3),
    generate_points(10000, 10, 2, 3),
    generate_points(10000, 10, 10, 3),
    generate_points(10000, 15, 20, 3),
    generate_points(10000, 20, 10, 3),
]

# huge_file 文件格式
# 所属簇中心, x坐标, y坐标

# 初始簇中心
centers = [
    (0, 2),
    (12, 14),
    (24, 12),
    (13, 3),
    (5, 4),
    (10, 19),
    (17, 19),
]

f = open("huge_data.txt", "w")

# 节点当前的中心*数据点坐标*所有的节点
line_format = "%s*%s*%s"
for i in range(len(points_list)):
    center = centers[i]
    points = points_list[i]
    for point in points:
        print(line_format%(center, point, centers), file=f)


#plot_points(points_list)
