from multiprocessing import Process, Queue
import linalg_helper as lh

EXIT = 0
INIT = 1
GEN_HASHCODE = 2
FIX_DIM = 3
ADD_TABLE = 4

#NUM_PROCESS = None
qin = []
qout = []
subprocesses = []
table2process = {}

multiprocess = True

def main_process(qin, qout):
    tables = []
    uniqueIds = []

    while True:
        cmd = qin.get()
        #print('command: ', cmd, end='')
        if cmd == EXIT:
            print('shutdown process')
            break

        if cmd == INIT:
            #print('multi-process: init')
            tables = []
            uniqueIds = []
            qout.put('init done')

        if cmd == ADD_TABLE:
            table = qin.get()
            id = qin.get()

            tables.append(table)
            uniqueIds.append(id)

        if cmd == GEN_HASHCODE:
            #print('Generate hashcode')
            point = qin.get()
            #print(point)
            tmp = {}
            for i in range(len(tables)):
                tmp[uniqueIds[i]] = lh.generateHashCodeGeneric(None, tables[i], point)
            qout.put(tmp)
        #print('finished')

def init_processes(num_processes):
    if not multiprocess:
        return

    #print("init_processes()")
    for p in range(num_processes):
        qin.append(Queue())
        qout.append(Queue())
        p = Process(target=main_process, args=(qin[p], qout[p]))
        p.start()
        subprocesses.append(p)



def split2process(tables, num_processes):
    if not multiprocess:
        return



    #print("split2process()")
    for n in range(num_processes):
        qin[n].put(INIT)

    for n in range(num_processes):
        qout[n].get()

    p = 0
    for n in range(len(tables)):
        table2process[n] = p
        qin[p].put(ADD_TABLE)
        qin[p].put(tables[n].hyperPlanes)
        qin[p].put(tables[n].unique_id)

        p += 1
        p = p % num_processes

    print('Multi process: the Table:Process list:', table2process)

def close_processes(num_processes):
    for p in range(num_processes):
        # print('wait for ', p, subprocesses[p])
        qin[p].put(EXIT)



def generateHashCodes_MP(session, point, num_processes):
    session.logger.entry("LSHForest.generateHashCodes_MP")
    for p in range(num_processes):
        qin[p].put(GEN_HASHCODE)
        qin[p].put(point.v)

    codes2 = {}
    #print('wait for results from processes')
    for p in range(num_processes):
        #print('wait for ', p, subprocesses[p])
        codes_tmp = qout[p].get()
        #print(codes_tmp)
        codes2 = {**codes2, **codes_tmp}

    session.logger.exit("LSHForest.generateHashCodes_MP")

    return codes2
