from __future__ import print_function
import sys
import time
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


class KVRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            print("got get request with value: %s" % self.path)
            value = _g_kvstorage.get(self.path)
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
            #print(key)
            #value = self.rfile.read(int(self.headers.get('content-length'))).decode()
            value = self.rfile.read(int(self.headers.get('content-length')))
            #print(value)
            _g_kvstorage.set(key, value)
            self.send_response(201)
            self.send_header("Content-type", "application/octet-stream")
            #self.send_header("Content-type", "text/plain")
            self.end_headers()
        except:
            pass


def main():
    if len(sys.argv) < 3:
        print('Usage: %s http_port dump_file.bin selfHost:port partner1Host:port partner2Host:port ...' % sys.argv[0])
        sys.exit(-1)

    if len(sys.argv) < 3:
        print('Usage: %s self_port partner1_port partner2_port ...' % sys.argv[0])
        sys.exit(-1)

    httpPort = int(sys.argv[1])
    print(httpPort)
    partners = ['localhost:%d' % int(p) for p in sys.argv[2:]]

    global _g_kvstorage
    _g_kvstorage = KVStorage('localhost:%d' % httpPort, partners)
    while True:
        time.sleep(0.5)

        if _g_kvstorage._getLeader() is not None:
            #_g_kvstorage.set('test', 2)
            #print(_g_kvstorage.get('test'))
            #print(_g_kvstorage.get_counter())
            print("Servers are synced")
            break
   
    #_g_kvstorage.set('test', 2)
    #print(_g_kvstorage.get('test'))
    httpServer = HTTPServer(('localhost', httpPort), KVRequestHandler)
    httpServer.serve_forever()


if __name__ == '__main__':
    main()