from pysyncobj import SyncObj, SyncObjConf, replicated
import numpy as np
import sys
try:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
except ImportError:
    from http.server import BaseHTTPRequestHandler, HTTPServer
sys.path.append("../")

class Server(SyncObj):
    def __init__(self, selfAddress, partnerAddress) -> None:
        cfg = SyncObjConf(dynamicMembershipChange = True)
        super(Server, self).__init__(selfAddress, partnerAddress, cfg)
        self.__data = {}

    @replicated
    def add_image(self, name, data):
        self.__data[name] = data

        # if name in __data -> append
        

    def get_image(self, name):
        return self.__data.get(name, None)

class Client():
    def __init__(self, name, server_list) -> None:
        self.__name = name
        self.__serverlist = server_list
        self.__connected = None

    def connect(self, server_index):
        self.__connected = self.__serverlist[server_index]

    def stream_data(self, name, data):
        self.__connected.add_image(name, data)

    def get_data(self, name):
        return self.__connected.get_image(name)

    def get_name(self):
        return(self.__name)

def main():
    num_servers = int(sys.argv[1])
    num_clients = int(sys.argv[2])
    print("starting simulation with " + str(num_servers) + " servers")
    print("starting simulation with " + str(num_clients) + " clients")

    server_list_name  = np.array(['server' + str(x) + ':4321' for x in range(num_servers)])
    client_list_name = np.array(['client' + str(x) for x in range(num_clients)])

    server_list = []
    for i, server in enumerate(server_list_name):
        new_server = Server(server, server_list_name[np.arange(num_servers) != i])
        server_list.append(new_server)

    client_list = []
    for i, client in enumerate(client_list_name):
        new_client = Client(client, server_list)
        client_list.append(new_client)

    client_list[0].connect(0)
    client_list[1].connect(0)
    client_list[2].connect(1)

    server_list[0].add_image('test', 1)
    print(server_list[0].get_image('test'))
    print(server_list[1].get_image('test'))

    client_list[0].stream_data('test', 1)
    print(client_list[0].get_data('test'))
    print(client_list[1].get_data('test'))
    print(client_list[2].get_data('test'))



if __name__ == '__main__':
    main()