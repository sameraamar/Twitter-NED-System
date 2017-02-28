from ProcessManager import AgentInterface, ServiceInterface
import codecs

DOC2THREAD = 10
INIT_FILE = 11

class DumpThreadAgent(AgentInterface):
    filename = ""
    file = None

    def handleCommand(self, cmd):

        print("DumpThreadAgent.handleCommand")

        if cmd == DOC2THREAD:
            id, leadId = self.queueIn.get()
            self.printDoc(id, leadId)

        if cmd == INIT_FILE:
            self.filename = self.queueIn.get()
            self.file = codecs.open(self.filename, mode='+w', encoding='utf-8')

        return

    def finish(self):
        print('calling finish DumpThreadAgent', self.name)

        self.file.close()

    def printDoc(self, id, leadId):
        line = '{0}\t{1}\t{2}\n'.format(id, leadId)
        self.file.write(line)

class DumpThreadService(ServiceInterface):
    def __init__(self, session, name):
        agentname = DumpThreadAgent.__module__ + '.' + DumpThreadAgent.__name__
        ServiceInterface.__init__(self, name, agentname, session=session)

    def printDoc(self, id, leadId):
        self.request(DOC2THREAD)
        self.request((id, leadId))

    def finish(self):
        print('calling finish DumpThreadService', self.name)
        ServiceInterface.finish()