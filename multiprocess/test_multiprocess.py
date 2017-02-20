from multiprocessing import Process, Queue
from scipy import sparse
import numpy as np
import time
from scipy.sparse import hstack
from scipy.sparse import vstack

thelist = range(1000*1000)

def main_process(qin, qout):
    while True:
        cmd = qin.get()
        if cmd == 0:
            break
        if cmd == 1:
            sublist = qin.get()
            qout.put(sum(sublist))
        if cmd == 2:
            i = qin.get()
            mat = sparse.lil_matrix((1, 1000), dtype=np.float64)
            mat[:] = i
            qout.put(mat)


def f(q, sublist):
    q.put(sum(sublist))

def main():
    start = 0
    chunk = 500*1000

    qout = []
    qin = []

    NP = 5

    chunk = int(len(thelist) / NP)
    subprocesses = []
    for n in range(NP):
        qin.append(Queue())
        qout.append(Queue())
        p = Process(target=main_process, args=(qin[n], qout[n]))
        p.start()
        subprocesses.append(p)

    for n in range(NP):
        qin[n].put(1)
        print(n*chunk, (n+1)*chunk-1)
        qin[n].put(thelist[n*chunk:(n+1)*chunk-1])

    total = 0
    for n in range(NP):
        val = qout[n].get()
        total += val

    print ("total is", total, '=', sum(thelist))





    mat = None
    for n in range(NP):
        qin[n].put(2)
        qin[n].put(n+1)

    for n in range(NP):
        temp = qout[n].get()
        print(temp)








    for n in range(NP):
        p = subprocesses[n]
        qin[n].put(0)

        p.join()

if __name__ == '__main__':
    main()