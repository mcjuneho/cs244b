from __future__ import print_function
from pickletools import read_unicodestring1
import sys
import this
import time
import random
from tokenize import Double
try:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
except ImportError:
    from http.server import BaseHTTPRequestHandler, HTTPServer
sys.path.append("../")
from pysyncobj import SyncObj, SyncObjConf, replicated

FRAGMENT_SIZE = 30


class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs):
        conf = SyncObjConf()
        super(KVStorage, self).__init__(selfAddress, partnerAddrs, conf)
        self.__data = {}

    @replicated
    def set(self, key, value):
        self.__data[key] = value

    # @replicated
    # def add_fragment(self, key, index, value):
    #     if (key in self.__data):
    #         self.__data[key] = value
    #     pass

    @replicated
    def pop(self, key):
        self.__data.pop(key, None)

    def get(self, key):
        return self.__data.get(key, None)
    
    def get_data(self):
        return self.__data

_g_kvstorage = None
assembler_storage = {}
drop_chance = 0.01

class Assembler():
    def __init__(self, size):
        print("entered initializing Assembler")
        self.__fragments = size // FRAGMENT_SIZE
        self.__size = size
        #self.__needed = set([i for i in range(size)])

        needed = [None for i in range(self.__fragments)] # index of fragment
        for i in range(self.__fragments):
            start = i * FRAGMENT_SIZE
            end = (i + 1) * FRAGMENT_SIZE
            if end + FRAGMENT_SIZE > self.__size:
                end = self.__size
            needed[i] = set([j for j in range(start, end)]) # packets for each needed fragment

        self.__needed = needed

        self.__data = [None for i in range(size)]
        print("finished initializing Assembler")


    def recieve(self, index, packet):
        fragment_idx = index // FRAGMENT_SIZE
        print("index received, ", index)
        if index in self.__needed[fragment_idx]:
            print("needed index")
            self.__data[index] = packet
            self.__needed[fragment_idx].remove(index)
            
        print("needed packets left in frag idx ", fragment_idx, " and num_left = ", len(self.__needed[fragment_idx]))
        print("left in needed packet = ", self.__needed[fragment_idx])
        return self.check_fragment_clear(fragment_idx)

        #return self.__needed
        # return self.get_needed()
    
    def check_fragment_clear(self, fragment_index):
        print("checked fragment is clear?")
        return len(self.__needed[fragment_index]) == 0

        # return self.get_needed()
        
    def finished(self):
        print("testing if finished")

        print(self.__needed)
        for i in range(self.__fragments):
            if (len(self.__needed[i]) != 0):
                return False
        return True

    def get_needed(self):
        #return self.__needed
        return set().union(*self.__needed)

    def reassemble(self):
        return b''.join(filter(None, self.__data))

class fragment_assembler():
    def __init__(self, size):
        self.__fragments = size // FRAGMENT_SIZE
        self.__size = size
        self.__needed = set([i for i in range(size)])
        self.__data = [None for i in range(self.__fragments)]

    def recieve(self, fragment_index, chunk):
        self.__data[fragment_index] = chunk
        start = fragment_index * FRAGMENT_SIZE
        end = (fragment_index + 1) * FRAGMENT_SIZE
        if end + FRAGMENT_SIZE > self.__size:
            end = self.__size

        self.__needed.remove([i for i in range(start, end+1)])

        return self.__needed

    def finished(self):
        
        return len(self.__needed) == 0

    def get_needed(self):
        return self.__needed

    def reassemble(self):
        return b''.join(filter(None, self.__data))


class KVRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            print("got get request with value: %s" % self.path)
            print(_g_kvstorage.get(self.path).finished())
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
                print("sent drop packet response")
                return
            assert(_g_kvstorage is not None)

            print("entered if tree")
            print("key is in data, ", key in _g_kvstorage.get_data())

            if key not in assembler_storage and key not in _g_kvstorage.get_data().keys():
                print("got to start of assembler")
                image_assembler = Assembler(size)
                print("made image assembler")
                assembler_storage[key] = image_assembler
                print("added assembler to storage")

            elif key not in assembler_storage and key in _g_kvstorage.get_data().keys():
                print("EXTRACTED ASSEMBLER FROM KVSTORAGE")
                assembler_storage[key] = _g_kvstorage.get(key)


            elif index != -1:
                print("got to start of non-start assembler")
                is_fragment_clear = assembler_storage[key].recieve(index, value)
                print("is_fragment_clear, ", is_fragment_clear)
                print("added data to assembler")
                if is_fragment_clear:
                    print("adding completed fragment packet to kvs")
                    _g_kvstorage.set(key, assembler_storage[key])
                needed = assembler_storage[key].get_needed()
                no_fragments_left = assembler_storage[key].finished()
                print("no_fragments_left, " ,no_fragments_left)
                if no_fragments_left:
                    # print("succesfully added complete assembler to kvs")
                    assembler_storage.pop(key)
                    needed = set()
                
            
            # print("left if tree")

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
            print(_g_kvstorage._getLeader())
            print("Servers are synced")
            break
   
    httpServer = HTTPServer(('localhost', httpPort), KVRequestHandler)
    httpServer.serve_forever()


if __name__ == '__main__':
    main()