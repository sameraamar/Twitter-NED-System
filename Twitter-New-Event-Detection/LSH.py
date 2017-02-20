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
        if self.session.logger != None:
            self.session.logger.entry('HashtableLSH.query')

        bucket = self.buckets[item['hashcode']]

        if self.session.logger != None:
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
        if self.session.logger!=None:
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

        if self.session.logger != None:
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

        if self.session.logger!=None:
            self.session.logger.entry("fix_dimension")

        a = new_dimension
        b = self.hyperPlanes.shape[1]

        delta = a-b
        ext = self.randomHyperPlanes(self.hyperPlanesNumber, delta)

        self.hyperPlanes = np.hstack((self.hyperPlanes, ext))

        #self.hyperPlanes = self.hyperPlanes.tocsr()
        self.saveHyperPlanes()

        if self.session.logger!=None:
            self.session.logger.exit("fix_dimension")


    def add(self, doc_point, hashcode=None):
        if self.session.logger!=None:
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
        
        if self.session.logger!=None:
            self.session.logger.exit('HashtableLSH.add')
        return item

    def saveHyperPlanes(self):
        lh.saveMatrix(self.unique_id, self.hyperPlanes)


    def randomHyperPlanes(self, hyperPlanesNumber, dimSize):
        key = 'HashtableLSH.randomHyperPlanes {0}'.format(dimSize)
        if self.session.logger!=None:
            self.session.logger.entry(key)

        planes = lh.randomMatrix(self.hyperPlanesNumber, dimSize, 1.0)

        #rvs = stats.norm(loc=0).rvs  #scale=2,
        #planes = sparse.random(hyperPlanesNumber, dimSize, format='lil', density=1.0, data_rvs=rvs)

        if self.session.logger!=None:
            self.session.logger.exit(key)
        return planes
   
    def size(self):
        return self.total

    def myprint(self):
        self.session.logger.info('total: {0}. number of buckets: {1}.  max bucke size: {2}. hyperplanes number: {3}'.format(self.total, len(self.buckets), self.maxBucketSize, self.hyperPlanesNumber))
        lengths = [len(self.buckets[b]) for b in self.buckets]
        self.session.logger.info('number of items in each bucket: {}'.format( lengths))


 
