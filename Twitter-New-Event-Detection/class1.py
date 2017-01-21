import time
import numpy as np
from scipy import sparse
from scipy import stats

def randomMatrix(rows, cols, density):
    rvs = stats.norm(loc=0).rvs  # scale=2,
    M = sparse.random(rows, cols, format='lil', density=density, data_rvs=rvs)
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
    m = matrix * vector.T
    m [m > 0] = 1
    m [m < 0] = 0
    txt = ''.join(m.A.ravel().astype('U1'))
    return txt

features = 1000
test_count = 6000

matrix = randomMatrix(13, features, 1.0)
vector = randomMatrix(1, features, 0.01)
vector = abs(vector)


methods = [ generateCode1, generateCode2 ]

for func in methods:
    print ('run {0} times of method {1}:'.format( test_count, func ) )
    time1 = time.time()
    for i in range(test_count):
        code1 = func(matrix, vector)
    time1 = time.time() - time1

    print ('\tcode: {0} - run time: '.format(code1), end="" )
    print ('{0}(s) == {1}(m) [ avergae {2}(s) ]'.format(time1, time1/60, time1/test_count) )