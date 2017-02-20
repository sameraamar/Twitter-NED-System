# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 12:51:47 2016

@author: SAMER AAMER
"""

from scipy import sparse
from session import Session
import time
from LSH import HashtableLSH

from multiprocessing import Process, Queue
import linalg_helper as lh

EXIT = 0
INIT = 1
GEN_HASHCODE = 2
FIX_DIM = 3
ADD_TABLE = 4
ADD_DOC_AND_QUERY = 5
DISTANCE = 6

#NUM_PROCESS = None
qin = []
qout = []
subprocesses = []
table2process = {}

def main_process(qin, qout):
    tables = []
    session = None

    while True:
        cmd = qin.get()
        #print('multi-proc: command: ', cmd, end='')
        if cmd == EXIT:
            #print('multi-proc: shutdown process')
            break

        if cmd == DISTANCE:
            id, left, right = qin.get()
            d = lh.distance(left, right, None, auto_fix_dim=True)
            qout.put( (id, d ) )

        if cmd == INIT:
            folder = qin.get()
            session = Session()
            session._temp_folder = folder

            numberTables = qin.get()
            maxBucketSize, dimensionSize, hyperPlanesNumber = qin.get()

            tables = [HashtableLSH(session, maxBucketSize, dimensionSize, hyperPlanesNumber) for i in range(numberTables)]

            qout.put('init done')

        if cmd == FIX_DIM:
            new_dim = qin.get()
            for t in tables:
                t.fix_dimension(new_dim)

            qout.put('done')

        if cmd == ADD_DOC_AND_QUERY:
            doc_point = qin.get()
            tmp = {}
            for t in tables:
                item = t.add(doc_point)
                neighbors = t.query(item)
                tmp[t.unique_id] = neighbors
            qout.put(tmp)

        ##print('multi-proc: finished')

def init_processes(session, num_processes, numberTables, params):

    print("init_processes: sending a request")
    for p in range(num_processes):
        qin.append(Queue())
        qout.append(Queue())
        p = Process(target=main_process, args=(qin[p], qout[p]))
        p.start()
        subprocesses.append(p)

    tables = {}
    p = 0
    for i in range(numberTables):
        tables[p] = tables.get(p, 0) + 1
        p+=1
        p = p % num_processes

    for n in range(num_processes):
        qin[n].put(INIT)

        qin[n].put(session.get_temp_folder())

        qin[n].put(tables[n])
        qin[n].put(params)

    print("init_processes: waiting for a response")
    for n in range(num_processes):
        qout[n].get()

    print("init_processes: finished")


def fix_dim(num_processes, new_dim):
    #print('multi-proc: fix_dim: entry')
    for n in range(num_processes):
        qin[n].put(FIX_DIM)
        qin[n].put(new_dim)

    for n in range(num_processes):
        qout[n].get()
    #print('multi-proc: fix_dim: exit')

"""
def split2process(tables, num_processes):


    p = 0
    for n in range(tables):
        table2process[n] = p
        qin[p].put(ADD_TABLE)
        qin[p].put(tables[n].hyperPlanes)
        qin[p].put(tables[n].unique_id)

        p += 1
        p = p % num_processes

    #print('multi-proc: Multi process: the Table:Process list:', table2process)
"""

counter_flag = 0
def add_and_query_doc(num_processes, doc_point):
    global counter_flag
    #print('multi-proc: add_and_query_doc: entry' , counter_flag)
    for n in range(num_processes):
        qin[n].put(ADD_DOC_AND_QUERY)
        qin[n].put(doc_point)

    neighbors = {}
    for n in range(num_processes):
        tmp = qout[n].get()
        neighbors = {**neighbors, **tmp}

    #print('multi-proc: add_and_query_doc: exit', counter_flag)
    counter_flag += 1
    return neighbors


def close_processes(num_processes):
    #print('multi-proc: closing')
    for p in range(num_processes):
        #print('multi-proc: wait for ', p, subprocesses[p])
        qin[p].put(EXIT)



def generateHashCodes_MP(session, point, num_processes):
    session.logger.entry("LSHForest_MP.generateHashCodes_MP")
    for p in range(num_processes):
        qin[p].put(GEN_HASHCODE)
        qin[p].put(point.v)

    codes2 = {}
    ##print('multi-proc: wait for results from processes')
    for p in range(num_processes):
        ##print('multi-proc: wait for ', p, subprocesses[p])
        codes_tmp = qout[p].get()
        #print(codes_tmp)
        codes2 = {**codes2, **codes_tmp}

    session.logger.exit("LSHForest_MP.generateHashCodes_MP")

    return codes2

class LSHForest_MP:
    """
    LSHForest - set of LSH HashTable.
    """
    # parameters
    dimSize = 3
    numberTables = 1

    # data members
    session = None

    num_processes = 1

    def __init__(self, **kwargs):
        self.dimSize = 3
        self.numberTables = 1
        self.session = None

        self.dimension_jumps = None
        self.num_processes = 1

        return super().__init__(**kwargs)


    def init(self, dimensionSize, session, num_processes = 2, dimension_jumps = 5000, hyperPlanesNumber=40, numberTables=4, maxBucketSize=10):
        self.session = session
        self.session.logger.entry('LSH.__init__')
        self.dimSize = dimensionSize

        self.num_processes = num_processes
        self.dimension_jumps = dimension_jumps

        self.numberTables = numberTables

        init_processes(session, self.num_processes, numberTables, (maxBucketSize, dimensionSize, hyperPlanesNumber))
        #split2process(self.numberTables, self.num_processes)

        self.session.logger.exit('LSH.__init__')


    def myprint(self):
        if self.session.logger == None:
            return
            
        for h in self.hList:
            self.session.logger.debug('*******************************************')
            h.myprint()
        self.session.logger.info('dimenion: {0} tables {1} '.format(self.dimSize, self.numberTables))

    def fix_dimension(self, point):
        self.session.logger.entry("LSHForest_MP.fix_dimension")

        if self.dimSize < point.shape[1]:
            self.dimSize = point.shape[1] + self.dimension_jumps
            self.session.logger.debug("Changing dimension to bigger: {0}".format(self.dimSize))

            fix_dim(self.num_processes, self.dimSize)


        if self.dimSize > point.shape[1]:
            newshape = (point.shape[0], self.dimSize)
            point = sparse.csr_matrix((point.data, point.indices, point.indptr), shape=newshape, copy=False)

        self.session.logger.exit("LSHForest_MP.fix_dimension")
        return point

    def add_and_query(self, doc_point):
        self.session.logger.entry('LSHForest_MP.add_and_query')
        neighbors_list = add_and_query_doc(self.num_processes, doc_point)

        c = len(neighbors_list)
        if c > self.numberTables:
            self.session.logger.info('Retreived {0} neighbors'.format(c))

        self.session.logger.exit('LSHForest_MP.add_and_query')
        return neighbors_list

    def add(self, doc_point):
        self.session.logger.entry('LSHForest_MP.add')

        nearest = None
        nearestDist = None
        comparisons = 0
        #invokes = []
        counter = {}
        items = {}


        doc_point.v = self.fix_dimension(doc_point.v)
        self.session.logger.entry('add_to_table')
        neighbors_list = self.add_and_query(doc_point)
        for x in neighbors_list:
            neighbors = neighbors_list[x]
            for candidate in neighbors:
                n = candidate['point']
                if doc_point.ID == n.ID:
                    continue

                c = counter.get(n.ID, 0)
                counter[n.ID] = c + 1
                if items.get(n.ID, None) == None:
                    items[n.ID] = candidate
            self.session.logger.exit('add_to_table')


        #self.session.logger.entry('add_to_table_2')
        #limit = 3 * self.numberTables

        #tmp = nearestDist
        #for id in sorted(counter, key=counter.get, reverse=True):
        #    dist = lh.distance(doc_point, items[id]['point'], self.session.logger, auto_fix_dim=True)
        #    if nearestDist==None or (dist != None and nearestDist>dist):
        #        nearest = items[id]
        #        nearestDist = dist

        #    limit -= 1
        #    if limit == 0:
        #        break
        ##print('**** nearest: ', nearestDist)
        #self.session.logger.exit('add_to_table_2')
        #nearestDist = tmp

        self.session.logger.entry('add_to_table_3')
        limit = 3 * self.numberTables
        compare_to = sorted(counter, key=counter.get, reverse=True)[0:limit]
        p = 0
        for id in compare_to:
            qin[p].put( DISTANCE1 )
            qin[p].put ( (id, doc_point, items[id]['point']) )
            p+=1
            p=p % self.num_processes

        p = 0
        for i in range(len(compare_to)):
            id, dist = qout[p].get()
            if nearestDist==None or (dist != None and nearestDist>dist):
                nearest = items[id]
                nearestDist = dist
            p+=1
            p=p % self.num_processes

        #print('nearest: ', nearestDist)

        self.session.logger.exit('add_to_table_3')

        self.session.logger.exit('LSHForest_MP.add')
        return nearest, nearestDist, comparisons, doc_point


    def size(self):
        return self.hList[0].size()

    def finish(self):
        close_processes(self.num_processes)
