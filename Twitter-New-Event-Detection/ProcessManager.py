from multiprocessing import Process, Queue
import time
from session import Session, human_time
from simplelogger import simplelogger


# -------------------

INIT = 0
EXIT = 1

processes = {}

# -----------------------


def import_class(cl):
    if cl.find('.') == -1:
        cl = __name__ + '.' + cl
    (modulename, classname) = cl.rsplit('.', 2)

    m = __import__(modulename, globals(), locals(), [classname])
    return getattr(m, classname)

def __initProcess__(name, agentname, temp_folder):
    assert (processes.get(name, None) is None)

    p = {}
    p['in'] = Queue()
    p['out'] = Queue()
    p['agent'] = agentname
    p['process'] = Process(target=main_process, args=(p['in'], p['out']))
    p['process'].start()

    processes[name] = p

    p['in'].put( (INIT, (name, agentname, temp_folder)) )




def main_process(qin, qout):
    agent = None
    base = time.time()
    name = ''
    session = None

    while True:
        cmd, params = qin.get()
        #print('Received command: ', cmd)
        if cmd == INIT:
            name, agentname, temp_folder_name = params

            session = Session()
            session._temp_folder = temp_folder_name
            log_filename = session.get_temp_folder() + '/log_{0}.log'.format(name)
            session.init_logger(filename=log_filename, std_level=simplelogger.INFO, file_level=simplelogger.DEBUG, profiling=False)

            if session.output is None:
                session.init_output()

            agent = import_class(agentname)(session, qin, qout)
            agent.setname(name)

            qout.put('Started')

        elif cmd == EXIT:
            agent.finish()

            timediff = time.time() - base
            print('my process {0} was live: {1}'.format(agentname, human_time( seconds=timediff)))
            break

        else:
            agent.handleCommand(cmd, params)


class AgentInterface:
    queueIn = None
    queueOut = None
    session = None
    name = ''

    def __init__(self, session, queueIn, queueOut):
        self.queueIn = queueIn
        self.queueOut = queueOut
        self.session = session
        return

    def setname(self, name):
        self.name = name

    def handleCommand(self, cmd, params):
        #print("AgentInterface.handleCommand")
        #params
        #print('command', cmd)
        self.queueOut.put('done')
        return

    def finish(self):
        self.queueOut.put('done')
        return


class ServiceInterface:
    agentname = ''
    name = ""
    session = None

    def __init__(self, name, agentname, session=None):
        '''
        example of ="AgentInterface" can be : Agent.__module__ + '.' + Agent.__name__
        :param session:
        :param name:
        :param agentname:
        '''
        self.agentname = agentname
        self.name = name
        self.session = session

    def start(self, tempfolder):
        assert (self.name != None)
        __initProcess__(self.name, self.agentname, tempfolder)

    def _check(self):
        if not processes[self.name]['process'].is_alive():
            raise Exception("I lost my child process: {0}".format(self.name))

    def request(self, cmd, *params):
        self._check()

        processes[self.name]['in'].put((cmd, params))

    def response(self):
        self._check()
        return processes[self.name]['out'].get()

    def queueSize(self):
        return processes[self.name]['in'].qsize()

    def finish(self):
        entry = processes.get(self.name, None)
        if (entry is None) or not entry['process'].is_alive():
            return False
        self.request(EXIT)
        return True

    def finish_response(self):
        res = self.response()
        processes.pop (self.name)
        return res


# -------------- SAMPLE IMPLEMENTATION --------------------
#------------ sum list of numbers -------------


class AgentSumList(AgentInterface):
    def handleCommand(self, cmd, params):
        #print("AgentSumList.handleCommand", cmd, params)
        if cmd == 5:
            start, end = params
            s = 0
            for i in range(start, end):
                s+=i
            self.queueOut.put(s)
        return



class ServiceSumList(ServiceInterface):
    def __init__(self, name):
        agentname = AgentSumList.__module__ + '.' + AgentSumList.__name__
        ServiceInterface.__init__(self, name, agentname)

    def sum_request(self, start, end):
        self.request(5, start, end)

    def sum_response(self):
        return self.response()



if __name__ == '__main__':


    #service = ServiceInterface("Samer")


    #service.start()
    #service.fire_request()
    #print(service.fire_response())

    proc = []
    for n in range(10):
        sumList = ServiceSumList("sum" + str(n))
        sumList.start('c:/temp')
        proc.append(sumList)
        print('Starting...')

    for n in range(10):
        sumList = proc[n]
        print(sumList.response())

    x = 1000000
    base = time.time()
    for n in range(10):
        sumList = proc[n]
        sumList.sum_request(x*n, x*(n+1))

    s = 0
    print('Calculate 1...')
    for p in proc:
        r = p.sum_response()
        print('\ttemp: ', r)
        s += r
    print('res1:', s)

    print('time: ', time.time() - base)

    base = time.time()
    s = 0
    print('Calculate 2...')
    for n in range(10):
        s2 = 0
        for j in range(x*n, x*(n+1)):
            s2 += j
        s += s2
        print('\ttemp: ', s2)
    print('res2:', s)
    print('time: ', time.time() - base)

    for p in proc:
        p.finish()


    #service.finish()

#