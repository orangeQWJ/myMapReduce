import xmlrpc.client
import config
#proxy = xmlrpc.client.ServerProxy("http://10.211.55.4:8000/")
rpc_server_address = "http://%s:%s/" % (config.master_ip, config.rpc_port)
proxy = xmlrpc.client.ServerProxy(rpc_server_address)

if __name__ == "__main__":
    print("3 is even: %s" % str(proxy.is_even('qiwenju', 18)))
    print("100 is even: %s" % str(proxy.is_even("nana", 17)))
    print("a + b = %s" % str(proxy.sum(1, 17)))
