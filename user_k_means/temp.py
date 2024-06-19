from lib.mapreduce import EmitIntermediate


def my_map(file_name):
    for line in open(file_name):
        ###
        ###
        ###
        ###
        EmitIntermediate(m_key, m_value)






def my_reduce(key, values):
    f = open("reduce_out.txt", "a")
    ######
    ######
    ######
    ######
    print(, file=f)








