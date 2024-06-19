def my_map(file_name):
    f = open("mid_data.txt", "w")
    for line in open(file_name):
        data = line.split("*")
        print(data[0], data[1], data[2] )
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
        mid_key = str(closest_center)
        mid_value = "*%s*%s" %(point, center_list)
        print(mid_key + mid_value, file=f)


my_map("./huge_data.txt")
