from urllib.request import parse_keqv_list
import pip._vendor.requests as requests
import time
import imageio.v3 as iio
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import random
import sys

class Client():
    def __init__(self, name, ports) -> None:
        self.__name = name
        self.__serverports = ports
        self.__connected = None

    def connect(self, server_index):
        self.__connected = 'http://localhost:' + self.__serverports[server_index]

    def stream_data(self, name, data, size, index):
        headers = {"Content-type": "application/octet-stream", "size": str(size), "index": str(index)}
        r = requests.post(self.__connected + '/' + name, data = data, headers = headers)
        if r.status_code == '400':
            return None
        return r.headers.get("parts-needed")

    def start_stream(self, name, size):
        headers = {"Content-type": "application/octet-stream", "size": str(size), "index": "-1"}
        r = requests.post(self.__connected + '/' + name, data = b'', headers = headers)
        return r.headers.get("parts-needed")

    def get_data(self, name):
        headers = {"Content-type": "application/octet-stream"}
        r = requests.get(self.__connected + '/' + name, headers = headers, stream=True)
        return r.raw.data

    def get_name(self):
        return(self.__name)

def main():
    partition_chance = float(sys.argv[1])
    print("partition chance: " + str(partition_chance))
    servers_ports = ['9200', '9300']

    client1 = Client('client1', servers_ports)
    client2 = Client('client2', servers_ports)

    client1.connect(0)
    client2.connect(1)

    #start streaming and retrieving data
    img = iio.imread('../photos/stanford_big.jpeg')
    jpg_encoded = iio.imwrite("<bytes>", img, extension=".jpeg")
    print(len(jpg_encoded))

    size = len(jpg_encoded)
    packets = []
    num_packets = size // 1000
    print(num_packets)
    #return
    #num_packets = 300

    for i in range(num_packets-1):
        #print(size//num_packets * i)
        packets.append(jpg_encoded[size//num_packets * i: size//num_packets * (i+1)])
    print(size//num_packets * (num_packets-1))
    packets.append(jpg_encoded[size//num_packets * (num_packets-1):])

    round = 0
    partitions = 0
    #partition_chance = 0.1

    start = time.time()

    packets_left = eval(client1.start_stream('test' + str(partitions), num_packets))

    while len(packets_left) != 0:
        print("round " + str(round))
        print("packets left: " + str(packets_left))
        if random.random() < partition_chance:
            print("network partition " + str(partitions))
            partitions += 1
            packets_left = eval(client1.start_stream('test' + str(partitions), num_packets))
            print(packets_left)

        round += 1

        for i in packets_left:
            new_packets_left = client1.stream_data('test' + str(partitions), packets[i], num_packets, i)
            if new_packets_left is not None:
                packets_left = min(eval(new_packets_left), packets_left)

        #sleep for 60 mils to simulate network rtt
        time.sleep(0.06)

    end = time.time()

    print(end-start)
    
    #time.sleep(3)

    #jpg_recieved = client2.get_data('test' + str(partitions))
    #img_received = iio.imread(jpg_recieved)
    #imgplot = plt.imshow(img_received)
    #plt.show()

    #print(client2.get_data('test'))

if __name__ == '__main__':
    main()