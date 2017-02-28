from scipy.sparse import hstack
import math
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import pairwise_distances
#from scipy.spatial import distance as dist
#import linalg_helper as lh
from scipy import sparse
from scipy import stats

cache = {}

class Document:
    v = None
    metadata = None
    word_counts = None
    ID = None
    _norm = None
    _found = 0

    def __init__(self, ID, vector, word_counts, metadata):
        self.ID = ID
        self.v = vector
        self.metadata = metadata
        self.word_counts = word_counts
        self._norm = None
        self._found = 0

    def norm(self, logger=None):
        if self._norm == None:
            loggerEntry(logger, "Document.norm")
            n = self.v.dot(self.v.T)
            self._norm = np.sqrt (n[0,0])
            loggerEntry(logger, "Document.norm", exit=True)

        return self._norm

#def norm(v, logger):
#    loggerEntry(logger, "linalg.norm")
#    n = v.dot(v.T)
#    ret = np.sqrt (n[0,0])
#    loggerEntry(logger, "linalg.norm", exit=True)
#    return ret


def loggerEntry(logger, f, exit=False):
    if logger != None:
        if exit:
            logger.exit(f)
        else:
            logger.entry(f)

def similarity_manual(a, b, return_angle=False, auto_fix_dim = False, logger=None):
    loggerEntry(logger, "linalg.similarity_manual")

    """
    if isinstance(a, Document):
        anorm = a.norm(logger)
        a = a.v
    else:
        anorm = lh.norm(a)

    if isinstance(b, Document):
        bnorm = b.norm(logger)
        b = b.v
    else:
        bnorm = lh.norm(b)
    """

    norms = a.norm(logger) * b.norm(logger)

    a = a.v
    b = b.v
    if auto_fix_dim and a.shape[1] != b.shape[1]:
        m = min(a.shape[1], b.shape[1])
        if m < a.shape[1]:
            a = a[:, : m]
        if m < b.shape[1]:
            b = b[:, : m]

    dotprod = np.dot(a, b.T)[0, 0]
    result = dotprod / norms

    if return_angle:
        degree = math.degrees( math.acos(result) )
        retVal = result, degree
    else:
        retVal = result

    loggerEntry(logger, "linalg.similarity_manual", exit=True)
    return retVal

def cosine_similarity_sklearn(a, b, return_angle=False, auto_fix_dim = False, logger=None):
    loggerEntry(logger, "linalg.cosine_similarity_sklearn")
    if auto_fix_dim and a.shape[1] != b.shape[1]:
        if a.shape[1] < b.shape[1]:
            c = a
            a = b
            b = c
        delta = a.shape[1] - b.shape[1]
        zeros = sparse.lil_matrix((1, delta), dtype=np.float64)
        b = hstack((b, zeros))

    res = cosine_similarity(a, b)[0][0]
    if res > 1.0 and res < 1.00001:
        res = 1.0

    if return_angle:
        degree = math.degrees( math.acos(res) )
        retVal = res, degree
    else:
        retVal = res

    loggerEntry(logger, "linalg.cosine_similarity_sklearn", exit=True)
    return retVal

#def cosine_similarity_wrapper(a, b, return_angle=False, auto_fix_dim = False, logger=None):
#    loggerEntry(logger, "linalg.cosine_similarity_wrapper")
#
#    ret = similarity_manual(a, b, return_angle=return_angle, auto_fix_dim=auto_fix_dim, logger=logger)
#
#    loggerEntry(logger, "linalg.cosine_similarity_wrapper", exit=True)
#    return ret


def distance(a, b, logger, auto_fix_dim = False):
    loggerEntry(logger, "linalg.distance")
    dist = 1.0-similarity_manual(a, b, auto_fix_dim=auto_fix_dim, logger=logger)
    loggerEntry(logger, "linalg.distance", exit=True)
    return dist

def saveMatrix(filename, matrix):
    file = open(filename + '.shape', 'w+')
    file.write( '\n'.join ( [ str(matrix.dtype), str(matrix.shape[0]), str(matrix.shape[1]) ] ) )
    file.close()

    fp = np.memmap(filename, dtype=matrix.dtype, mode='write', shape=matrix.shape)
    fp[:] = matrix[:]
    del fp

def loadMatrix(filename):
    file = open(filename + '.shape', 'r')
    dtype = file.readline().strip()
    rows = int(file.readline())
    cols = int(file.readline())
    file.close()

    fpr = np.memmap(filename, dtype=dtype, mode='readonly', shape=(rows, cols))

    return fpr


def randomMatrixDense(rows, cols):
    return np.random.uniform(-1.0, 1.0, [rows, cols])

def randomMatrix(rows, cols, density):
    if density == 1.0:
        return randomMatrixDense(rows, cols)

    rvs = stats.norm(loc=0).rvs  # scale=2,
    M = sparse.random(rows, cols, format='csr', density=density, data_rvs=rvs)
    return M


def generateHashCodeGeneric(session, planes, point):
    if session != None:
        session.logger.entry('HashtableLSH.generateHashCodeGeneric')

    # temp_hashcode = lh.generateCode(planes, point)

    # assert(planes.shape[1] >= point.shape[1])
    if planes.shape[1] > point.shape[1]:
        newshape = (point.shape[0], planes.shape[1])
        point = sparse.csr_matrix((point.data, point.indices, point.indptr), shape=newshape, copy=False)
        # matrix = matrix[:,:vector.shape[1]]

    m = planes * point.T
    # txt1 = ''.join(['1' if d >= 0 else '0' for d in m])

    m[m > 0] = 1
    m[m < 0] = 0

    if session != None:
        session.logger.exit("HashtableLSH.generateHashCodeGeneric")

    txt = ''.join([str(int(k)) for k in m[:, -1]])

    #txt = ''.join(m.A.ravel().astype('U1'))
    return txt


#def generateHashCodes(self, planes, point):
#    self.session.logger.exit("HashtableLSH.generateHashCode")
#    txt = generateHashCodeGeneric(hyperPlanes, point)
#
#    self.session.logger.exit("HashtableLSH.generateHashCode")#
#
#    return txt
#

def generateCode(matrix, vector):
    return generateCode3(matrix, vector)

def generateCode2(matrix, vector):
    m = tmp * vector.T
    m[m > 0] = 1
    m[m < 0] = 0
    txt = ''.join(m.A.ravel().astype('U1'))
    return txt


def generateCode3(matrix, vector):
    assert(matrix.shape[1] == vector.shape[1])

    if matrix.shape[1] > vector.shape[1]:
        newshape = (vector.shape[0], matrix.shape[1])
        vector = sparse.csr_matrix((vector.data, vector.indices, vector.indptr), shape = newshape, copy=False)
        #matrix = matrix[:,:vector.shape[1]]

    m = matrix * vector.T
    m[m > 0] = 1
    m[m < 0] = 0
    txt = ''.join([str(int(k)) for k in m[:,-1]])
    return txt


def generateCode1(matrix, vector):
    nonzeros = np.nonzero(vector)
    temp_hashcode = []

    for i in range(matrix.shape[0]):
        d = 0
        for j in nonzeros[1]:
            d += matrix[i, j] * vector[0, j]

        temp_hashcode.append('1' if d > 0 else '0')

    return ''.join(temp_hashcode)


def randomPoint(features):
    return randomSamples(1, features)

def randomSamples(samples, features):
    #rvs = stats.randint(low=1, high=5).rvs  # .norm(scale=2, loc=0).rvs
    #rvs =  numpy.random.randn
    rvs = stats.norm(loc=0).rvs  # scale=2,
    # S = sparse.random(1, dim, density=0.25, data_rvs=rvs)
    den = min(10.0 / (features * samples), 0.1)
    if den * features < 1.0:
        den = 1.0 / features
    S = sparse.random(samples, features, format='csr', density=den, data_rvs=rvs)
    S = abs(S)
    # stats.poisson(10, loc=0).rvs
    # rvs = stats.randint.stats(0, 100, moments='mvsk')
    return S



if __name__ == '__main__':
    import time
    def test(txt, av, bv, log=False):
        if log:
            print('=====================')
            print(txt)
            print('p1', av)
            print('p2', bv)

        a = av
        if isinstance(av, Document):
            a = av.v

        b = bv
        if isinstance(bv, Document):
            b = bv.v

        delta = a.shape[1] - b.shape[1]
        zeros = sparse.lil_matrix((1, delta), dtype=np.float64)
        b1 = hstack((b, zeros))

        basetime = time.time()
        if log:
            print('pairwise_distances:', pairwise_distances(a, b1, metric="cosine"))
        times['pairwise_distances'] = times.get('pairwise_distances', 0) + (time.time()-basetime)

        #basetime = time.time()
        #print('angular_distance:', angular_distance(a, b1))
        #times['angular_distance'] = times.get('angular_distance', 0) + (time.time()-basetime)

        basetime = time.time()
        if log:
            print('cosine_similarity_sklearn:', cosine_similarity_sklearn(a, b, return_angle=True, auto_fix_dim=True))
        times['cosine_similarity_sklearn'] = times.get('cosine_similarity_sklearn', 0) + (time.time()-basetime)

        basetime = time.time()
        if log:
            print('manual similarity:', similarity_manual(av, bv, return_angle=True, auto_fix_dim=True))
        times['manual similarity'] = times.get('manual similarity', 0) + (time.time() - basetime)

        times['count'] += 1

    times = {}
    times['count'] = 0


    #%%

    for i in range(1):
        a = randomPoint(300)
        b = randomPoint(200)

        test('Test 0: ', a, b)

    a = [1, 2, 0.0003]
    b = [1, 2, 0]

    a = np.reshape(a, (1, -1))
    b = np.reshape(b, (1, -1))

    test( 'Test 1: ', a, b)

    a = randomPoint(30)
    b = randomPoint(20)

    test( 'Test 2: ', a, b)


    a = [0.9096448781658965, 0.4068440185937889, 0]
    b = [-1.5069740305430366, 0, 0]
    a = np.reshape(a, (1, -1))
    b = np.reshape(b, (1, -1))

    test( 'Test 3(a): ', a, b)


    a = [0.9096448781658965, 0.4068440185937889, 0]
    b = [-1.5069740305430366, 0, 1]
    a = np.reshape(a, (1, -1))
    b = np.reshape(b, (1, -1))

    test( 'Test 3(b): ', a, b)

    a = sparse.lil_matrix((1, 3), dtype=np.float64)
    a[0, 0] = 0.9096448781658965
    b = sparse.lil_matrix((1, 3), dtype=np.float64)
    b[0, 0] = -1.5069740305430366


    test( 'Test 4: ', a, b)

    a = sparse.lil_matrix((1, 3), dtype=np.float64)
    a[0, 0] = 0.9096448781658965
    b = sparse.lil_matrix((1, 3), dtype=np.float64)
    b[0, 0] = 8.660254038
    b[0, 1] = 5

    test( 'Test 5: ', a, b)

    a = sparse.lil_matrix((1, 10), dtype=np.float64)
    a[0, 0] = 8.660254038
    a[0, 3] = 5
    a[0, 5] = 0.19911282712717281914
    b = a.copy()
    b[0, 5] = 0.199112827127172819146

    test( 'Test 6: ', a, b)

    a = sparse.lil_matrix((1, 31), dtype=np.float64)
    a[0, 4] = 0.317146009
    a[0, 10] = 0.317146009
    a[0, 24] = 0.951438026
    a[0, 25] = 0.951438026
    a[0, 26] = 0.951438026
    a[0, 27] = 0.951438026
    a[0, 28] = 0.951438026
    a[0, 29] = 0.951438026
    a[0, 30] = 0.951438026

    b = sparse.lil_matrix((1, 31), dtype=np.float64)
    b[0, 0] = 0.417021884
    b[0, 3] = 0.417021884
    b[0, 4] = 0.417021884
    b[0, 5] = 0.417021884
    b[0, 6] = 0.834043767
    b[0, 7] = 0.834043767
    b[0, 8] = 0.834043767
    b[0, 9] = 0.834043767
    b[0, 10] = 0.834043767
    b[0, 11] = 0.834043767
    b[0, 12] = 0.834043767

    a = Document('111', a)
    b = Document('222', b)

    test( 'Test 7: ', a, b, log=True)


    import pprint
    for t in times.keys():
        if t != 'count':
            times[t] = 1.0 * times[t] / 1.0 #times['count']
    pprint.pprint(times)