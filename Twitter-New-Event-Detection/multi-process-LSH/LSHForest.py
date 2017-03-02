# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 12:51:47 2016

@author: SAMER AAMER
"""

from scipy import sparse
import numpy as np
import time
import linalg_helper as lh
from session import uniqueTempFileName
from LSH import HashtableLSH

class LSHForest:
    """
    LSHForest - set of LSH HashTable.
    """
    # parameters
    dimSize = 3

    dimension_jumps = 0
    numberTables = 1

    # data members
    hList = None
    _planes = None
    _planes_idx = None
    _planes_file = None
    session = None

    def __init__(self, **kwargs):
        self.dimSize = 3
        self.numberTables = 1
        self.hList = None
        self.session = None


        self._planes = None
        self._planes_idx = None
        self._planes_file = None
        return super().__init__(**kwargs)


    def init(self, dimensionSize, session, dimension_jumps = 5000, hyperPlanesNumber=40, numberTables=4, maxBucketSize=10):
        self.session = session
        self.session.logger.entry('LSH.__init__')
        self.session.logger.info('LSH model being initialized')
        self.dimSize = dimensionSize
        self.dimension_jumps = dimension_jumps
        self._planes = None
        self._planes_idx = None
        self._planes_file = None

        self.numberTables = numberTables


        self.hList = [HashtableLSH(session, maxBucketSize, dimensionSize, hyperPlanesNumber) for i in range(numberTables)]
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
        codes = self.generateHashCodes(doc_point)

        for table in self.hList:
            self.session.logger.entry('add_to_table')
            item = table.add(doc_point.ID, doc_point.v, hashcode=codes[table.unique_id])
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
        #mp.close_processes()
        return

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


 
