#import psyco
#psyco.full()

import time
import numpy as np
#from heapdict import doc
from scipy import sparse
from scipy import stats
import random
import linalg_helper as la

def randomMatrixDense(rows, cols):
    m = np.random.uniform(-1.0, 1.0, [rows, cols])
    return m

def randomMatrix(rows, cols, density):
    rvs = stats.norm(loc=0).rvs  # scale=2,
    M = sparse.random(rows, cols, format='csr', density=density, data_rvs=rvs)
    return M

def generateCode1(matrix, vector):
    nonzeros = np.nonzero(vector)
    temp_hashcode = []

    for i in range(matrix.shape[0]):
        d = 0
        for j in nonzeros[1]:
            d += matrix[i, j] * vector[0, j]

        temp_hashcode.append('1' if d > 0 else '0')

    return ''.join(temp_hashcode)

def generateCode2(matrix, vector):
    if matrix.shape[1] > vector.shape[1]:
        #print('change dim')
        newshape = (vector.shape[0], matrix.shape[1])
        vector = sparse.csr_matrix((vector.data, vector.indices, vector.indptr), shape=newshape, copy=False)


    m = matrix * vector.T
    m [m > 0] = 1
    m [m < 0] = 0

    if type(m) == np.ndarray:
        #n = 0
        #for x in m:
        #    n += x
        #    n = n >> 1
        #txt = n
        txt = ''.join([str(int(k)) for k in m[:,-1]])
    else:
        txt = ''.join(m.A.ravel().astype('U1'))

    import resource
    def using(point=""):
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return '''%s: usertime=%s systime=%s mem=%s mb
               ''' % (point, usage[0], usage[1],
                      (usage[2] * resource.getpagesize()) / 1000000.0)

    return txt


import pymongo
from pymongo import MongoClient
import pymongo
import time, json

"""
from scipy.sparse import csr_matrix
def _dict_to_csr(term_dict, shape):
    term_dict_v = term_dict.values()
    term_dict_k = term_dict.keys()
    term_dict_k_zip = zip(*term_dict_k)
    term_dict_k_zip_list = list(term_dict_k_zip)

    #shape = (len(term_dict_k_zip_list[0]), len(term_dict_k_zip_list[1]))
    data = list(term_dict_v)
    rows_cols = list(map(list, zip(*term_dict_k)))
    data = (data, rows_cols)
    csr = csr_matrix(data, shape=shape)
    return csr
"""


def generateHashCodeMain(filename, point_str):
    point = str2sparse(point_str)
    mat = la.loadMatrix(filename)

    hashcode = generateCode2(mat, point)
    return hashcode

def generateHashCodeMainMP(pairs, q):
    for filename, point_str in pairs:
        hashcode = generateHashCodeMain(filename, point_str)
        q.put(hashcode)

def sparse2str(mm):
    return '::'.join([ str(mm.data), str(mm.indices), str(mm.indptr), str(mm.dtype), str(mm.shape) ])


def str2sparse(txt):
    values = txt.split('::')

    data = np.fromstring(values[0][1:-1], sep=' ')
    indices = np.fromstring(values[1][1:-1], sep=' ', dtype=int)
    indptr = np.fromstring(values[2][1:-1], sep=' ', dtype=int)
    dtype = values[3]
    newshape = np.fromstring(values[4][1:-1], sep=', ', dtype=int)

    mat = sparse.csr_matrix((data, indices, indptr), shape=newshape, dtype=dtype, copy=False)

    return mat


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
    from subprocess import Popen

    features = 7000
    test_count = 1


    matrices = [] # [ randomMatrixDense(13, features) ]

    import os, fnmatch

    directory = "C:\\Users\\samera\\AppData\\Local\\Temp\\LSH\\mytest"
    for filename in os.listdir( directory ):
        if fnmatch.fnmatch(filename, '*.dat'):
            filename = os.path.join(directory, filename)
            fpr = la.loadMatrix(filename)
            matrices.append(fpr)
            #break


    vector = randomMatrix(1, random.randint(features/2, features ) , 10.0/features)
    vector = randomMatrix(1, features , 0.01)
    vector = abs(vector)


    methods = [ generateCode2 ]
    processes1 = []

    print('==============')
    code1 = []
    for func in methods:
        time1 = time.time()
        print('Method {1}: run {0} times on {2} matrices :'.format(test_count, func, len(matrices)))
        for i in range(test_count):
            for matrix in matrices:
                #print("matrix...")
                code1.append( func(matrix, vector) )
                processes1.append( Process(target=generateCode2, args=(matrix, vector, ))  )
                #print ('\tcode: {0} - run time: '.format(code1), end="" )

        time1 = time.time() - time1
        print ('{0}(s) == {1}(m) [ avergae {2}(s) ]'.format(time1, time1/60, time1/test_count) )

    print('==============')
    code2 = []
    vector_str = sparse2str(vector)
    for func in methods:
        time1 = time.time()
        print('Method {1}: run {0} times on {2} matrices :'.format(test_count, func, len(matrices)))
        for i in range(test_count):
            for filename in os.listdir(directory):
                if fnmatch.fnmatch(filename, '*.dat'):
                    filename = os.path.join(directory, filename)
                    code2.append ( generateHashCodeMain(filename, vector_str) )
                    #break
        time1 = time.time() - time1
        print('{0}(s) == {1}(m) [ avergae {2}(s) ]'.format(time1, time1 / 60, time1 / test_count))

    """
    print('==============')
    code3 = []
    vector_str = sparse2str(vector)
    n = 80
    vector_str = '\n'.join([vector_str[i:i+n] for i in range(0, len(vector_str), n)])

    subprocesses = []

    basetime = time.time()
    print('Method {1}: run {0} times on {2} matrices :'.format(test_count, 'batch mode', len(matrices)))
    for i in range(test_count):
        for filename in os.listdir(directory):
            if fnmatch.fnmatch(filename, '*.dat'):
                filename = os.path.join(directory, filename)
                cmd1 = 'python -c "import  class2; ' #class2.generateHashCodeMain (\'{0}\', \'{1}\')"'.format(filename, vector_str)
                subprocesses.append(Popen(cmd1, shell=True))
                #code3.append(generateHashCodeMain(filename, vector_str))
                #break

    time1 = time.time() - basetime
    print('{0}(s) == {1}(m) [ avergae {2}(s) ]'.format(time1, time1 / 60, time1 / test_count))

    for p in subprocesses:
        p.wait()

    time1 = time.time() - basetime
    print('{0}(s) == {1}(m) [ avergae {2}(s) ]'.format(time1, time1 / 60, time1 / test_count))
    """

    print(code1)
    print(code2)

    q = mp.Queue()

    info('main line')
    ps = [ Process(target=f, args=('bob', q,)), Process(target=f, args=('mariam', q,)), Process(target=f, args=('samer', q,)) ]
    for p in ps:
        p.start()

    for p in ps:
        p.join()

    while not q.empty():
        print (q.get())


    #print(code3)

    print('==============')
    prcoesses = []
    q = mp.Queue()
    vector_str = sparse2str(vector)
    time1 = time.time()
    pairs = []
    print('Method {1}: run {0} times on {2} matrices :'.format(test_count, "multiprocessing", len(matrices)))
    for i in range(test_count):
        for filename in os.listdir(directory):
            if fnmatch.fnmatch(filename, '*.dat'):
                filename = os.path.join(directory, filename)
                pairs.append( (filename, vector_str) )
                # break

    cpus = 2
    for i in range(cpus):
        i1 = len(matrices) / cpus * i
        i1 = int(i1)
        i2 = len(matrices) / cpus * (i+1) - 1
        i2 = int(i2)
        print(i1, i2)
        prcoesses.append(Process(target=generateHashCodeMainMP, args=(pairs[i1:i2], q,)))


    time1 = time.time()
    print('Method {1}: run {0} times on {2} matrices with {3} processes:'.format(test_count, "multiprocessing", len(matrices), len(processes1)))
    for p in processes1:
        p.start()

    for p in processes1:
        p.join()

    while not q.empty():
        print (q.get())

    time1 = time.time() - time1
    print('{0}(s) == {1}(m) [ avergae {2}(s) ]'.format(time1, time1 / 60, time1 / test_count))
