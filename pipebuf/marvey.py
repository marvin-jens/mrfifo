import argparse
import os
import sys
from time import time
import struct
import multiprocessing as mp
from pipebuf_futex import PipeBufFutex

def reader(buf_path):
    pb = PipeBufFutex.open(buf_path)
    while line := pb.read_line() is not None:
        pass #print('.', end=None)

    print("done reading")
    pb.close()

def writer(buf_path, msg_size=4096, n=100000, n_every=1000000):
    pb = PipeBufFutex.open(buf_path)
    msg = bytes("A" * msg_size, encoding='ascii')
    T0 = time()
    for i in range(n):
        pb.write_line(msg)
        if (i > 0) and (i % n_every == 0):
            T = time()
            dT = T - T0
            rate = n_every*msg_size/1024/1024/dT
            print(f"writing with {rate:.2f} MB/second")
            T0 = T
    
    print("done writing")
    pb.close_for_write()


def main(msg_size=4096, n=5000000, buf_path='bla', capacity=4096*1024):
    pb = PipeBufFutex.create(buf_path, capacity)
    p_read = mp.Process(target=reader, kwargs=dict(buf_path=buf_path))
    p_write = mp.Process(target=writer, kwargs=dict(buf_path=buf_path, n=n))

    p_write.start()
    p_read.start()

    p_read.join()
    p_write.join()

    pb.unlink()

if __name__ == "__main__":
    main()