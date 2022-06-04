from __future__ import print_function
from pickletools import read_unicodestring1
import sys
import time
import random
from tokenize import Double
try:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
except ImportError:
    from http.server import BaseHTTPRequestHandler, HTTPServer
sys.path.append("../")
from pysyncobj import SyncObj, SyncObjConf, replicated


class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs):
        conf = SyncObjConf()
        super(KVStorage, self).__init__(selfAddress, partnerAddrs, conf)
        self.__data = {}

    @replicated
    def set(self, key, value):
        self.__data[key] = value

    @replicated
    def pop(self, key):
        self.__data.pop(key, None)

    def get(self, key):
        return self.__data.get(key, None)

_g_kvstorage = None
assembler_storage = {}
drop_chance = 0.01

class Assembler():
    def __init__(self, size):
        self.__size = size
        self.__needed = set([i for i in range(size)])
        self.__data = [None for i in range(size)]

    def recieve(self, index, chunk):
        if index in self.__needed:
            self.__data[index] = chunk
            self.__needed.remove(index)

        return self.__needed

    def finished(self):
        return len(self.__needed) == 0

    def get_needed(self):
        return self.__needed

    def reassemble(self):
        return b''.join(self.__data)


class KVRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            print("got get request with value: %s" % self.path)
            value = _g_kvstorage.get(self.path).reassemble()
            #print(value)

            if value is None:
                self.send_response(404)
                #self.send_header("Content-type", "text/plain")
                self.end_headers()
                return

            self.send_response(200)
            #self.send_header("Content-type", "text/plain")
            self.send_header("Content-type", "application/octet-stream")
            self.end_headers()
            #self.wfile.write(value.encode('utf-8'))
            #self.wfile.write(str(value).encode())
            self.wfile.write(value)
        except:
            pass

    def do_POST(self):
        try:
            print("got post request with value: %s" % self.path)
            key = self.path
            #value = self.rfile.read(int(self.headers.get('content-length'))).decode()
            value = self.rfile.read(int(self.headers.get('content-length')))
            print("read value")

            #get size of the packet here
            size = int(self.headers.get('size'))
            index = int(self.headers.get('index'))
            needed = set(i for i in range(size))

            if random.random() < drop_chance and index != -1:
                self.send_response(400)
                self.end_headers()
                print("sent response")
                return

            #print(size)
            #print(index)
            #print(needed)

            if key not in assembler_storage:
                #print("got to start of assembler")
                image_assembler = Assembler(size)
                #print("made image assembler")
                assembler_storage[key] = image_assembler
                #print("added assembler to storage")
            elif index != -1:
                #print("got to start of non-start assembler")
                needed = assembler_storage[key].recieve(index, value)
                #print("added data to assembler")
                if len(needed) == 0:
                    print("adding completed packet to kvs")
                    _g_kvstorage.set(key, assembler_storage[key])
                    print("succesfully added complete assembler to kvs")
                    assembler_storage.pop(key)

            #print("sending response")
            self.send_response(201)
            self.send_header("Content-type", "application/octet-stream")
            self.send_header("parts-needed", str(needed))
            #self.send_header("Content-type", "text/plain")
            self.end_headers()
        except:
            pass

def main():
    global _g_kvstorage
    global assembler_storage
    global drop_chance

    if len(sys.argv) < 3:
        print('Usage: %s http_port dump_file.bin selfHost:port partner1Host:port partner2Host:port ...' % sys.argv[0])
        sys.exit(-1)

    if len(sys.argv) < 3:
        print('Usage: %s self_port partner1_port partner2_port ...' % sys.argv[0])
        sys.exit(-1)

    httpPort = int(sys.argv[1])
    print(httpPort)
    drop_chance = float(sys.argv[2])
    print("drop chance: " + str(drop_chance))
    partners = ['localhost:%d' % int(p) for p in sys.argv[3:]]

    _g_kvstorage = KVStorage('localhost:%d' % httpPort, partners)
    while True:
        time.sleep(0.5)

        if _g_kvstorage._getLeader() is not None:
            print("Servers are synced")
            break
   
    #_g_kvstorage.set('test', 2)
    #print(_g_kvstorage.get('test'))
    httpServer = HTTPServer(('localhost', httpPort), KVRequestHandler)
    httpServer.serve_forever()


if __name__ == '__main__':
    main()