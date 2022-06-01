import pip._vendor.requests as requests
import time
import imageio.v3 as iio
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

class Client():
    def __init__(self, name, ports) -> None:
        self.__name = name
        self.__serverports = ports
        self.__connected = None

    def connect(self, server_index):
        self.__connected = 'http://localhost:' + self.__serverports[server_index]

    def stream_data(self, name, data):
        headers = {"Content-type": "application/octet-stream"}
        requests.post(self.__connected + '/' + name, data = data, headers = headers)

    def get_data(self, name):
        headers = {"Content-type": "application/octet-stream"}
        r = requests.get(self.__connected + '/' + name, headers = headers, stream=True)
        return r.raw.data

    def get_name(self):
        return(self.__name)

def main():
    servers_ports = ['9200', '9300']

    client1 = Client('client1', servers_ports)
    client2 = Client('client2', servers_ports)

    client1.connect(0)
    client2.connect(1)

    #start streaming and retrieving data
    img = iio.imread('../photos/stanford.jpeg')
    jpg_encoded = iio.imwrite("<bytes>", img, extension=".jpeg")
    client1.stream_data('test', jpg_encoded)
    time.sleep(0.5)

    jpg_recieved = client2.get_data('test')
    img_received = iio.imread(jpg_recieved)
    imgplot = plt.imshow(img_received)
    plt.show()

    #print(client2.get_data('test'))

if __name__ == '__main__':
    main()