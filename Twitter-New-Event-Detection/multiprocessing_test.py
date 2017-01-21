import multiprocessing as mp
from multiprocessing import Process
import os

def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

def f(name, q):
    info('function f')
    print('hello', name)
    q.put(name)

if __name__ == '__main__':
    q = mp.Queue()

    info('main line')
    ps = [ Process(target=f, args=('bob', q,)), Process(target=f, args=('mariam', q,)), Process(target=f, args=('samer', q,)) ]
    for p in ps:
        p.start()

    for p in ps:
        p.join()

    while not q.empty():
        print (q.get())