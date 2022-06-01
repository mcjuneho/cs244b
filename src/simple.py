from __future__ import print_function

import sys
import time
from functools import partial
sys.path.append("../")
from pysyncobj import SyncObj, replicated


class TestObj(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(TestObj, self).__init__(selfNodeAddr, otherNodeAddrs)
        self.__counter = 0
        self.__data = {}

    @replicated
    def incCounter(self):
        self.__counter += 1
        return self.__counter

    @replicated
    def addValue(self, value, cn):
        self.__counter += value
        return self.__counter, cn

    @replicated
    def set(self, key, value):
        self.__data[key] = value

    def get(self, key):
        return self.__data.get(key)

    def getCounter(self):
        return self.__counter


def onAdd(res, err, cnt):
    print('onAdd %d:' % cnt, res, err)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: %s self_port partner1_port partner2_port ...' % sys.argv[0])
        sys.exit(-1)

    port = int(sys.argv[1])
    partners = ['localhost:%d' % int(p) for p in sys.argv[2:]]
    o = TestObj('localhost:%d' % port, partners)
    n = 0
    old_value = -1
    while True:
        # time.sleep(0.005)
        time.sleep(0.5)
        #if o.getCounter() != old_value:
            #old_value = o.getCounter()
            #print(old_value)
        if o._getLeader() is None:
            print("no leader")
            continue
            
        o.set('test', 1)
        print(o.get('test'))

        # if n < 2000:
        #if 3n < 20:
            #o.addValue(10, n, callback=partial(onAdd, cnt=n))
            #print("added value")
        #n += 1