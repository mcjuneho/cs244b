from pysyncobj import SyncObj, SyncObjConf, replicated
import numpy as np
import sys

class Server(SyncObj):
    def __init__(self, selfAddress, partnerAddress) -> None:
        cfg = SyncObjConf(dynamicMembershipChange = True)
        super(Server, self).__init__(selfAddress, partnerAddress, cfg)
        self.__data = {}

    @replicated
    def add_image(self, name, data):
        self.__data[name] = data

    def get(self, name):
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

    def get_name(self):
        return(self.__name)

def main():
    num_servers = int(sys.argv[1])
    num_clients = int(sys.argv[2])
    print("starting simulation with 3 servers")

    server_list_name  = np.array(['server' + str(x) + ':4321' for x in range(num_servers)])
    client_list_name = np.array(['client' + str(x) for x in range(num_clients)])
    print(server_list_name)

    server_list = []
    for i, server in enumerate(server_list_name):
        new_server = Server(server, server_list_name[np.arange(num_servers) != i])
        server_list.append(new_server)
    print(server_list)

    client_list = []
    for i, client in enumerate(client_list_name):
        new_client = Client(client, server_list)
        client_list.append(new_client)
    print(client_list)



if __name__ == '__main__':
    main()