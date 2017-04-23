# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 12:51:47 2016

@author: SAMER AAMER
"""

from scipy import sparse
from session import Session
from multiprocessing import Process, Queue
import linalg_helper as lh
from simplelogger import simplelogger
from session import human_time
import numpy as np
#from LSHForest import LSHForest
from LSH import HashtableLSH

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

def main_process(qin, qout):
    tables = []
    session = None
    #lashForest = LSHForest()

    while True:
        cmd = qin.get()
        #print('multi-proc: command: ', cmd, end='')
        if cmd == EXIT:
            #print('multi-proc: shutdown process')
            #temp = session.get_temp_folder()
            #session.logger.profiling_dump(human_time, path=temp)

            session.finish()
            break

        if cmd == DISTANCE:
            id, left, right = qin.get()
            d = lh.distance(left, right, None, auto_fix_dim=True)
            qout.put( (id, d ) )

        if cmd == INIT:
            folder = qin.get()
            session = Session()
            session._temp_folder = folder

            log_filename = session.get_temp_folder() + '/log_subprocess.log'
            session.init_logger(filename=log_filename, std_level=simplelogger.INFO, file_level=simplelogger.DEBUG,profiling=True)

            numberTables = qin.get()
            maxBucketSize, dimensionSize, hyperPlanesNumber = qin.get()

            #lashForest.init(session=session, dimensionSize=dimensionSize , numberTables=numberTables,
            #              #dimension_jumps=dimension_jumps,
            #              hyperPlanesNumber=hyperPlanesNumber, maxBucketSize=maxBucketSize)

            tables = [HashtableLSH(session, maxBucketSize, dimensionSize, hyperPlanesNumber) for i in range(numberTables)]
            #print('Tables: ', len(tables))

            qout.put('init done')

        if cmd == FIX_DIM:
            new_dim = qin.get()
            for t in tables:
                t.fix_dimension(new_dim)

            qout.put('done')

        if cmd == ADD_DOC_AND_QUERY:
            point_vec = qin.get()
            ID = point_vec.ID
            if ID == '':
                print('Tables: ', len(tables))
            #point_vec = qin.get()

            tmp = {}

            #-----------
            for t in tables:
                item = t.add(ID, point_vec.v)
                neighbors = t.query(item)
                tmp[t.unique_id] = neighbors
            #-----------

            #nearest, nearestDist, comparisons, doc_point = lashForest.add(point_vec)
            #qout.put([nearest])

            qout.put(tmp)


        ##print('multi-proc: finished')

def init_processes(session, num_processes, numberTables, params):
    global qin
    global qout
    global subprocesses

    qin = []
    qout = []
    subprocesses = []

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

#counter_flag = 0
def add_and_query_doc(num_processes, doc_point, session):
    #global counter_flag
    #print('multi-proc: add_and_query_doc: entry' , counter_flag)

    session.logger.entry("M_P: add_and_query_doc()")

    for n in range(num_processes):
        qin[n].put(ADD_DOC_AND_QUERY)
        qin[n].put(doc_point)

    neighbors = {}
    to_handle = np.array(range(num_processes)).tolist()
    while len(to_handle) > 0:
        for n in to_handle:
            if not qout[n].empty():
                tmp = qout[n].get()
                neighbors = {**neighbors, **tmp}
                to_handle.remove(n)

    #print('multi-proc: add_and_query_doc: exit', counter_flag)
    #counter_flag += 1

    session.logger.exit("M_P: add_and_query_doc()")


    return neighbors


def close_processes(num_processes):
    #print('multi-proc: closing', num_processes)
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
    #id2doc = {}

    # data members
    session = None

    num_processes = 1
    thr = None

    def __init__(self, **kwargs):
        self.dimSize = 3
        self.numberTables = 1
        self.session = None
        #self.id2doc = {}
        self.dimension_jumps = None
        self.num_processes = 1


        return super().__init__(**kwargs)


    def init(self, dimensionSize, session, num_processes = 2, dimension_jumps = 5000, hyperPlanesNumber=40, numberTables=4, maxBucketSize=10):
        self.session = session
        self.session.logger.entry('LSH.__init__')
        self.dimSize = dimensionSize

        self.num_processes = num_processes
        self.dimension_jumps = dimension_jumps
        #self.id2doc = {}
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
        neighbors_list = add_and_query_doc(self.num_processes, doc_point, self.session)

        c = len(neighbors_list)
        if c > self.numberTables:
            self.session.logger.info('Retreived {0} neighbors'.format(c))

        self.session.logger.exit('LSHForest_MP.add_and_query')
        return neighbors_list

    def add(self, doc_point):
        self.session.logger.entry('LSHForest_MP.add')

        nearest = None
        nearestDist = None
        #invokes = []
        counter = {}

        #self.id2doc[doc_point.ID] = doc_point

        doc_point.v = self.fix_dimension(doc_point.v)
        self.session.logger.entry('add_to_table_1')
        neighbors_list = self.add_and_query(doc_point)
        self.session.logger.exit('add_to_table_1')
        self.session.logger.entry('add_to_table_2')
        for x in neighbors_list:
            neighbors = neighbors_list[x]
            for candidate in neighbors:
                if doc_point.ID == candidate:
                    continue

                c = counter.get(candidate, 0)
                counter[candidate] = c + 1

        limit = 3 * self.numberTables
        compare_to = sorted(counter, key=counter.get, reverse=True)[0:limit]

        self.session.logger.exit('add_to_table_2')
        nearest = nearestDist = None

        """
        self.session.logger.entry('add_to_table_3')
        for id in compare_to:
            dist = lh.distance(doc_point, self.thr.id2document[id], self.session.logger, auto_fix_dim=True)
            if nearestDist==None or (dist != None and nearestDist>dist):
                nearest = self.thr.id2document[id]
                nearestDist = dist

            limit -= 1
            if limit == 0:
                break
        self.session.logger.exit('add_to_table_3')
        """
        #print('NEAREST3: ', doc_point.ID, nearest, nearestDist)

        #print('NEAREST4: ', doc_point.ID, nearest, nearestDist)

        self.session.logger.exit('LSHForest_MP.add')
        return compare_to, doc_point


    def size(self):
        return self.hList[0].size()

    def finish(self):
        close_processes(self.num_processes)
