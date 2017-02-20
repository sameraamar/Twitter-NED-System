# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 12:51:47 2016

@author: SAMER AAMER
"""

import math
from scipy import stats
from scipy import sparse
import numpy as np
import time
from scipy.sparse import hstack
from scipy.sparse import vstack
import linalg_helper as lh
from session import uniqueTempFileName
import multi_process_worker as mp

#DIMENSION_JUMPS = None

class HashtableLSH:
    # parameters
    maxBucketSize = None
    hyperPlanesNumber = None

    # data members
    hyperPlanes = None
    buckets = None
    session = None
    total = 0

    unique_id = None

    def __init__(self, session, maxBucketSize, dimensionSize, hyperPlanesNumber):
        self.hyperPlanesNumber = hyperPlanesNumber
        self.maxBucketSize = maxBucketSize
        self.buckets = {}
        self.total = 0
        self.hyperPlanesNumber = hyperPlanesNumber
        self.session = session
        self.hyperPlanes = self.randomHyperPlanes( hyperPlanesNumber, dimensionSize )

        self.unique_id = uniqueTempFileName(self.session.get_temp_folder())
        self.saveHyperPlanes()

    def query(self, item):
        """
         find nearest neighbor
         :param item: in the same structure as returned by add function
         :return: closest approximate neighobor
         """
        self.session.logger.entry('HashtableLSH.query')
        bucket = self.buckets[item['hashcode']]

        self.session.logger.exit('HashtableLSH.query')
        return bucket

    def query1(self, item):
        """
        find nearest neighbor
        :param item: in the same structure as returned by add function
        :return: closest approximate neighobor
        """
        self.session.logger.entry('HashtableLSH.query')
        bucket = self.buckets[item['hashcode']]

        nearest = None
        minDist = None
        bucket_size = len(bucket)
        p1 = item['point']

        for neighbor in bucket:
            if neighbor['point'].ID == item['point'].ID:
                continue

            p2 = neighbor['point']

            dist = lh.distance(p1, p2, logger=self.session.logger, auto_fix_dim=True)
            if minDist == None or dist < minDist:
                minDist = dist
                nearest = neighbor

        self.session.logger.exit('HashtableLSH.query')
        return nearest, minDist, bucket_size

    def generateHashCode(self, point):
        self.session.logger.entry('HashtableLSH.generateHashCode')

        #temp_hashcode = lh.generateCode(self.hyperPlanes, point)

        #assert(self.hyperPlanes.shape[1] >= point.shape[1])
        if self.hyperPlanes.shape[1] > point.shape[1]:
            newshape = (point.shape[0], self.hyperPlanes.shape[1])
            point = sparse.csr_matrix((point.data, point.indices, point.indptr), shape=newshape, copy=False)
            # matrix = matrix[:,:vector.shape[1]]

        m = self.hyperPlanes * point.T
        #txt1 = ''.join(['1' if d >= 0 else '0' for d in m])

        m[m > 0] = 1
        m[m < 0] = 0
        txt2 = ''.join([str(int(k)) for k in m[:, -1]])

        self.session.logger.exit("HashtableLSH.generateHashCode")

        return txt2

    def generateHashCode_bak(self, point):
        self.session.logger.debug('Shape of point: {0} and nonzeros {1} point is {2} '.format(str(point.shape), len(np.nonzero(point)[0]), str(point) ))

        self.session.logger.entry('HashtableLSH.generateHashCode-a')

        nonzeros = np.nonzero(point)
        temp_hashcode = []

        for i in range(self.hyperPlanesNumber):
            d = 0
            v = list()
            for j in nonzeros[1]:
                v.append( self.hyperPlanes[i, j] * point[0,j] )

            d = sum(v)

            #temp_hashcode += ('1' if d > 0 else '0')
            temp_hashcode.append('1' if d > 0 else '0')
            #temp_hashcode = temp_hashcode.append(d)
        self.session.logger.exit("HashtableLSH.generateHashCode-a")

        return ''.join(temp_hashcode)

    def fix_dimension(self, new_dimension):
        if new_dimension <= self.hyperPlanes.shape[1]:
            return

        self.session.logger.entry("fix_dimension")

        a = new_dimension
        b = self.hyperPlanes.shape[1]

        delta = a-b
        ext = self.randomHyperPlanes(self.hyperPlanesNumber, delta)

        self.hyperPlanes = np.hstack((self.hyperPlanes, ext))

        #self.hyperPlanes = self.hyperPlanes.tocsr()
        self.saveHyperPlanes()

        self.session.logger.exit("fix_dimension")


    def add(self, doc_point, hashcode=None):
        self.session.logger.entry('HashtableLSH.add')
        if hashcode == None:
            hashcode = self.generateHashCode(doc_point.v)

        item = {}
        #item['ID'] = ID
        item['point'] = doc_point
        item['hashcode'] = hashcode

        b = self.buckets.get(hashcode, None)
        if b == None:
            self.buckets[hashcode] = []

        #search for closest item
#        minDist = 1
#        for k in b:
#            dist = distance_cosine(k['point'], point)
#            if dist < minDist:
#                #k['similar_count'] += 1
#                #item['similar_count'] += 1
#                minDist = dist
#        item['dist'] = minDist

        self.total += 1
        self.buckets[hashcode].append( item ) 
        
        if len(self.buckets[hashcode]) > self.maxBucketSize:
            #self.buckets[hashcode].pop(index=0) # = self.buckets[hashcode][1:]
            self.buckets[hashcode] = self.buckets[hashcode][1:]
            self.total -= 1
        
        #self.buckets[hashcode] = b
        
        self.session.logger.exit('HashtableLSH.add')        
        return item

    def saveHyperPlanes(self):
        lh.saveMatrix(self.unique_id, self.hyperPlanes)


    def randomHyperPlanes(self, hyperPlanesNumber, dimSize):
        key = 'HashtableLSH.randomHyperPlanes {0}'.format(dimSize)
        self.session.logger.entry(key)

        planes = lh.randomMatrix(self.hyperPlanesNumber, dimSize, 1.0)

        #rvs = stats.norm(loc=0).rvs  #scale=2,
        #planes = sparse.random(hyperPlanesNumber, dimSize, format='lil', density=1.0, data_rvs=rvs)

        self.session.logger.exit(key)
        return planes
   
    def size(self):
        return self.total

    def myprint(self):
        self.session.logger.info('total: {0}. number of buckets: {1}.  max bucke size: {2}. hyperplanes number: {3}'.format(self.total, len(self.buckets), self.maxBucketSize, self.hyperPlanesNumber))
        lengths = [len(self.buckets[b]) for b in self.buckets]
        self.session.logger.info('number of items in each bucket: {}'.format( lengths))

class LSHForest:
    """
    LSHForest - set of LSH HashTable.
    """
    # parameters
    dimSize = 3
    numberTables = 1

    # data members
    hList = None
    _planes = None
    _planes_idx = None
    _planes_file = None
    session = None

    multiprocess = True
    num_processes = 1

    def __init__(self, **kwargs):
        self.dimSize = 3
        self.numberTables = 1
        self.hList = None
        self.session = None

        self.dimension_jumps = None
        self.multiprocess = True
        self.num_processes = 1

        self._planes = None
        self._planes_idx = None
        self._planes_file = None
        return super().__init__(**kwargs)


    def init(self, dimensionSize, session, multiprocess = False, num_processes = None, dimension_jumps = 5000, hyperPlanesNumber=40, numberTables=4, maxBucketSize=10):
        self.session = session
        self.session.logger.entry('LSH.__init__')
        self.session.logger.info('LSH model being initialized')
        self.dimSize = dimensionSize

        self.multiprocess = multiprocess
        self.num_processes = num_processes
        self.dimension_jumps = dimension_jumps

        self._planes = None
        self._planes_idx = None
        self._planes_file = None

        self.numberTables = numberTables


        self.hList = [HashtableLSH(session, maxBucketSize, dimensionSize, hyperPlanesNumber) for i in range(numberTables)]

        if self.multiprocess:
            mp.init_processes(self.num_processes)
            mp.split2process(self.hList, self.num_processes)

        self.session.logger.info('LSH model initialization done')
        self.session.logger.exit('LSH.__init__')


    def myprint(self):
        if self.session.logger == None:
            return
            
        for h in self.hList:
            self.session.logger.debug('*******************************************')
            h.myprint()
        self.session.logger.info('dimenion: {0} tables {1} '.format(self.dimSize, self.numberTables))

    def fix_dimension(self, point):
        self.session.logger.entry("LSHForest.fix_dimension")

        if self.dimSize < point.shape[1]:
            self.dimSize = point.shape[1] + self.dimension_jumps
            self.session.logger.debug("Changing dimension to bigger: {0}".format(self.dimSize))
            for table in self.hList:
                table.fix_dimension(self.dimSize)

            mp.split2process(self.hList, self.num_processes)

            if self._planes_file!=None:
                del self._planes
                self._planes = None
                self._planes_idx = None
                self._planes_file = None

        if self.dimSize > point.shape[1]:
            newshape = (point.shape[0], self.dimSize)
            point = sparse.csr_matrix((point.data, point.indices, point.indptr), shape=newshape, copy=False)

        self.session.logger.exit("LSHForest.fix_dimension")
        return point

    def _concatenateHyperplanes(self, filename):
        self.session.logger.entry("LSHForest._concatenateHyperplanes")
        #file = open(filename + '.hp', 'w+')
        fp = None
        codes = {}
        i = 0
        for t in self.hList:
            h = t.hyperPlanes

            codes[t.unique_id] = i

            if i==0:
                newshape = (h.shape[0]*len(self.hList), h.shape[1])
                fp = np.memmap(filename, dtype=h.dtype, mode='w+', shape=newshape, order='C')
                fp[0: h.shape[0], :] = h
                i += 1
                continue


            fp[i*h.shape[0] : (i+1)*h.shape[0], : ] = h
            i += 1
            #file.write('\n'.join([str(matrix.dtype), str(matrix.shape[0]), str(matrix.shape[1])]))

        self.session.logger.exit("LSHForest._concatenateHyperplanes")
        return fp, codes

    def generateHashCodes(self, point):
        self.session.logger.entry("LSHForest.generateHashCodes")


        if self._planes_file == None:
            self._planes_file = uniqueTempFileName(self.session.get_temp_folder())
            self._planes, self._codes = self._concatenateHyperplanes(self._planes_file)

        txt = lh.generateHashCodeGeneric(self.session, self._planes, point.v)

        codes = {}

        for h in self.hList:
            i = self._codes[h.unique_id]
            codes[h.unique_id] = txt[i*h.hyperPlanesNumber : (i+1)*h.hyperPlanesNumber]

        self.session.logger.exit("LSHForest.generateHashCodes")

        return codes


    def generateHashCodes_old(self, point):
        self.session.logger.entry("LSHForest.generateHashCodes")

        codes = {}
        for table in self.hList:
            self.session.logger.entry("table.generateHashCodes")
            c = table.generateHashCode(point.v)
            self.session.logger.exit("table.generateHashCodes")
            codes[table.unique_id] = c

        self.session.logger.exit("LSHForest.generateHashCodes")
        return codes

    def add(self, doc_point):
        self.session.logger.entry('LSHForest.add')
        """add a point to the hash table
        the format of the point is assumed to be parse so it will be in libSVM format
        json {word:count, word:cout}"""
        nearest = None
        nearestDist = None
        comparisons = 0
        #invokes = []
        counter = {}
        items = {}


        doc_point.v = self.fix_dimension(doc_point.v)
        if self.multiprocess:
            codes = mp.generateHashCodes_MP(self.session, doc_point, self.num_processes)
        else:
            codes = self.generateHashCodes(doc_point)

        for table in self.hList:
            self.session.logger.entry('add_to_table')
            item = table.add(doc_point, hashcode=codes[table.unique_id])
            #item = table.add(doc_point)
            neighbors = table.query(item)
            for candidate in neighbors:
                n = candidate['point']
                if doc_point.ID == n.ID:
                    continue

                c = counter.get(n.ID, 0)
                counter[n.ID] = c + 1
                if items.get(n.ID, None) == None:
                    items[n.ID] = candidate
            self.session.logger.exit('add_to_table')

        limit = 3 * self.numberTables

        for id in sorted(counter, key=counter.get, reverse=True):
            dist = lh.distance(doc_point, items[id]['point'], self.session.logger, auto_fix_dim=True)
            if nearestDist==None or (dist != None and nearestDist>dist):
                nearest = items[id]
                nearestDist = dist

            limit -= 1
            if limit == 0:
                break


        self.session.logger.exit('LSHForest.add')
        return nearest, nearestDist, comparisons, doc_point


    def size(self):
        return self.hList[0].size()

    def finish(self):
        mp.close_processes(self.num_processes)

#%%


if __name__ == '__main__':  
    from simplelogger import simplelogger 
    from time import time
    
    def test3():
        # create logger
        logger = simplelogger()
        logger.init('c:/temp/file2.log', file_level=simplelogger.DEBUG, std_level=simplelogger.ERROR, profiling=True)
        
        print('Running...')

        n = 4
        d = 2**n
        dim = 50000
        maxB = 500
        tables = 50
        nruns = 10
        
        ll = LSHForest()

        ll.init(dimensionSize=dim, logger=logger, numberTables=tables, hyperPlanesNumber=d, maxBucketSize=maxB)

        print('finished init...')
        ID = 100
        start = before = time()
        for run in range(nruns):
            p = lh.randomPoint(dim)
            
            logger.debug('Generated point D{0}:\n{1}'.format(ID, p))
            
            ll.add('D'+str(ID), p)



            ID += 1
            dim += 1
            #perc = 10000.0*run/nruns
            if (run == nruns-1) or run % 100 == 0:
                t = time()-before
                perc = 100*run / nruns
                logger.info('%.2f %% (time: %.8f) - size %2d' % (perc, t, ll.size()))
                before = time()
    
        print('Done!', time()-start)
        ll.myprint()

        logger.profiling_dump()


    def test4():
        import sys, random

        sys.path.append("../lshash")
        import lshash
        from lshash import LSHash

        print('trying something else...')

        dim = 50000
        lsh = LSHash(hash_size=16, input_dim=dim, num_hashtables=20)

        before = time()

        for i in range(1000):
            lsh.index([random.randint(0, 4) for k in range(dim)])

        # lsh.index([2,3,4,5,6,7,8,9])
        # lsh.index([10,12,99,1,5,31,2,3])

        print(len(lsh.query([random.randint(0, 4) for k in range(dim)], distance_func="cosine")))

        print('Done.', time() - before)


    test3()
    #test4()


 
