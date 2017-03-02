import linalg_helper as lh
from ProcessManager import AgentInterface, ServiceInterface
from tweet_threads import TweetThread
import codecs

MAP_TO_THREAD = 6
ADD = 8
EXPIRE = 7
SETTINGS = 9
DUMP_THREADS = 10

class ClusteringAgent(AgentInterface):
    id2document = {}

    def __init__(self, queueIn, queueOut):
        self.id2document = {}
        AgentInterface.__init__(self, queueIn, queueOut)

    def handleCommand(self, cmd):
        try:

            if cmd == MAP_TO_THREAD:
                id, point, compare_to = self.queueIn.get()

                res = self.find_closest(id, point, compare_to)

                #print('clostest: ', res)
                self.queueOut.put(res)
                #print('sent ...', res)

            if cmd == EXPIRE:
                id = params
                self.id2document.pop(id)

            if cmd == ADD:
                id, document = params
                self.id2document[id] = document

        except Exception as e:
            print(e)
            raise

        return

    def find_closest(self, id, point, compare_to):
        min_d = None
        min_id = None
        for other in compare_to:
            right = self.id2document[other]
            d = lh.distance(point, right, None, auto_fix_dim=True)
            if (min_d is None) or min_d > d:
                min_d = d
                min_id = other

        return min_d, min_id



class ClusteringManagerProcess(ServiceInterface):

    def __init__(self, name, agentname=ClusteringAgent.__module__ + '.' + ClusteringAgent.__name__):
        print(agentname)
        ServiceInterface.__init__(self, name, agentname)

    def match_to_cluster(self, id, point, compare_to):
        self.request(MAP_TO_THREAD)
        self.request((id, point, compare_to))

    def match_to_cluster_response(self):
        return self.response()

    def add(self, id, document):
        self.request(ADD)
        self.request((id, document))

    def expire(self, id):
        self.request(EXPIRE)
        self.request((id))



class ClusteringManager:
    id2document = {}
    id2thread = {}
    proc = []
    num = 4

    def __init__(self):
        self.id2document = {}
        self.id2thread = {}
        self.num = 4

        self.proc = [ClusteringManagerProcess("mngr" + str(i)) for i in range(self.num)]

    def start(self):
        for p in self.proc:
            p.start()

    def finish(self):
        for p in self.proc:
            p.finish()

    def match_to_cluster(self, id, point, compare_to):
        self.id2document[id] = point

        nearestId = None
        nearestDist = None
        p_idx = 0
        opr = {}
        for c in compare_to:
            tmp = opr.get(p_idx, None)
            if tmp is None:
                opr[p_idx] = [c]
            else:
                opr[p_idx].append(c)
            p_idx += 1
            p_idx %= len(self.proc)

        for p_idx in opr:
            self.proc[p_idx].match_to_cluster(id, point, opr[p_idx])

        for p_idx in opr:
            p = self.proc[p_idx]
            min_d, min_id = p.match_to_cluster_response()
            if nearestId == None or nearestDist>min_d:
                nearestId = min_id
                nearestDist = min_d

        return nearestId, nearestDist

    def expire(self, id):
        self.id2thread.pop(id)
        self.id2document.pop(id)


        for p in self.proc:
            p.expire(id)


    def add(self, id, document):
        #self.id2thread[id] = document
        self.id2document[id] = document


        for p in self.proc:
            p.add(id, document)


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

class MasterClusteringAgent(AgentInterface):
    id2document = {}
    id2threadLead = {}
    id2thread2 = {}
    recent_docs = []
    counter = 0

    threshold = 0.5
    recent_size = 10
    max_thread_delta_time = 3600



    def __init__(self, session, queueIn, queueOut):
        self.id2document = {}
        self.id2threadLead = {}
        self.id2thread2 = {}
        self.counter = 0

        self.threshold = 0.5
        self.recent_size = 10
        AgentInterface.__init__(self, session, queueIn, queueOut)


    def handleCommand(self, cmd, params):
        try:
            if cmd == SETTINGS:
                #print(self.threshold)

                self.threshold = params

                #print(self.threshold)


            if cmd == MAP_TO_THREAD:
                id, point, compare_to = params #self.queueIn.get()

                self.map_to_thread(id, point, compare_to)
                self.counter += 1

                if self.counter % 100 == 0:
                    self.session.logger.debug('Process {0} has processed {1} items (in queue {2})'.format(self.name, self.counter, self.queueIn.qsize()))

            if cmd == EXPIRE:
                id = params
                self.id2document.pop(id)

            if cmd == DUMP_THREADS:
                filename, max_threads = params
                self.printThreads(filename, max_threads)

            if cmd == ADD:
                #print(self.queueIn)
                id, document = params
                self.id2document[id] = document


                self.recent_docs.append(id)
                if len(self.recent_docs) > self.recent_size:
                    self.recent_docs.pop(0)


        except Exception as e:
            print(e)
            raise

        return

    def map_to_thread(self, id, document, compare_to):
        self.session.logger.entry("map_to_thread")
        min_doc, min_dist = self.find_closest(id, document, compare_to)
        min_doc2, min_dist2 = self.searchInRecentDocs(id, document)

        if min_dist == None:
            min_dist = min_dist2
            min_doc = min_doc2

        elif min_dist2 is not None and min_dist2 < min_dist:
            min_dist = min_dist2
            min_doc = min_doc2

        create_new_thread = True
        if min_dist is not None and min_dist < self.threshold:
            create_new_thread = False

        if not create_new_thread:
            leadId = self.id2threadLead.get(min_doc.ID, None)
            nearThread = self.id2thread2.get(leadId, None)

            can_add = True

            if nearThread is not None:
                can_add = nearThread.can_add(document.metadata['timestamp'])

            if nearThread is None or not can_add:
                create_new_thread = True

        if create_new_thread:
            data = document.metadata
            self.id2threadLead[id] = id
            self.id2thread2[id] = TweetThread(self.session, id, document.word_counts, data['user'], data['timestamp'], max_time_delta=self.max_thread_delta_time)

            #msg = '*** NEW THREAD ***: new leader is {0} ("{1}"). '.format(id, self.text_metadata[id])


            #if nearestDist != None:
            #    msg += '\n\t***Nearest thread leader is {0} with distance {2} (threshold {3}): ("{1}").'.format(nearestID,
            #                                                                           self.text_metadata[nearestID]['text'],
            #                                                                           nearestDist,
            #                                                                           self.threshold)



        else:
            leadId = self.id2threadLead[min_doc.ID]
            nearThread = self.id2thread2[leadId]

            mydata = document.metadata
            leaddata = self.id2document[leadId].metadata

            nearThread.append(id, document.word_counts, mydata['user'], mydata['timestamp'], min_doc.ID, min_dist)
            self.id2threadLead[id] = leadId

            self.session.logger.debug(
                '*** EXISTING THREAD ***: Add document {0} ("{1}") to existing thread {2} ("{3}").\n\t@@@Nearest document is {4} with distance {6}: ("{5}").'.format(
                    id, mydata['text'], leadId, leaddata['text'],
                    min_doc.ID, min_doc.metadata['text'], min_dist))

        self.session.logger.entry("clean-thread-list")
        remove_me = []
        for th_id in self.id2thread2:
            thread = self.id2thread2[th_id]

            too_old = not thread.can_add(document.metadata['timestamp'])
            if too_old:
                thread.dump(self.session.output, self.id2document)
                self.session.logger.debug("Removing {0}: too old = {1}, size = {2}".format(th_id,
                                                                                           too_old,
                                                                                           thread.size()))
                remove_me.append(th_id)

            else:
                break # no need to proceed further in the loop

        if len(remove_me)>0:
            self.session.logger.debug("Released {0} old/closed clusters ".format(len(remove_me)))

        for th_id in remove_me:
            thread = self.id2thread2.pop(th_id)
            for tmpid in thread.idlist:
                self.id2threadLead.pop(tmpid)
        self.session.logger.exit("clean-thread-list")


        self.session.logger.exit("map_to_thread")



    def find_closest(self, id, document, compare_to):
        self.session.logger.entry("find_closest")

        min_doc = min_dist = None
        for cid in compare_to:
            cdoc = self.id2document[cid]
            dist = lh.distance(document, cdoc, self.session.logger, auto_fix_dim=True)
            if min_dist==None or (dist != None and min_dist>dist):
                min_doc = cdoc
                min_dist = dist

        self.session.logger.exit("find_closest")
        return min_doc, min_dist


    def searchInRecentDocs(self, id, document):
        self.session.logger.entry("searchInRecentDocs")
        nearestDist = None
        nearest = None
        # compare d to a fixed number of most recent documents
        flag = False
        for other in self.recent_docs:
            if other == id:
                continue

            # tmp = la.angular_distance(ID, other, doc, self.counts[self.doc_indices[other], :])
            other_doc = self.id2document[other]
            tmp = lh.distance(document, other_doc, logger=self.session.logger, auto_fix_dim=True)
            if nearestDist == None or nearestDist > tmp:
                nearestDist = tmp
                nearest = other_doc
                flag = True
        #if flag:  # found a new neighbor
        #    print(
        #        '*** Search in Recent Documents: ***: {0} was found to be close to {1} (distance: {2}).'.format(document.ID, nearest.ID, nearestDist) )

        self.session.logger.exit("searchInRecentDocs")
        return nearest, nearestDist

    def helper_lambda(self, x):
        return '-'.join([str(self.id2thread2[x].entropy()), str(self.id2thread2[x].users_count())])

    def printThreads(self, filename, max_threads):
        ttt = -1 #human_time(seconds=self.last_timestamp - self.first_timestamp)
        thr = 1
        firstime = True
        file = None

        for x in sorted(self.id2thread2, key=lambda x: self.helper_lambda(x), reverse=True):
            threadSize = self.id2thread2[x].size()

            entropy = self.id2thread2[x].entropy()
            if entropy < 2:
                continue

            if firstime:
                filename += 'aa.txt'

                file = codecs.open(filename, 'w', encoding='utf-8')
                file.write('Collected {1} threads. Printing threads with entropy > 3. total period: {0}\n'.format(
                    ttt, min(max_threads, len(self.id2thread2))))
                firstime = False

            # if threadSize<3:
            #    #not interesting anymore
            #    break

            self.session.logger.debug('Thread: {0}, size: {1} documents'.format(x, threadSize))
            text = self.text_metadata[x]['text']  # .replace('\t', ' ')
            # text = text.encode(encoding='utf-8')
            ttt = human_time(seconds=self.id2thread2[x].thread_time())
            isOpen = ''
            if not self.id2thread2[x].is_open():
                isOpen = ' [CLOSED]'

            file.write(
                '\n' + '-' * 40 + ' THREAD {0}{5} - {1} documents score: {2} and {3} users. period of {4}'.format(thr,
                                                                                                                  threadSize,
                                                                                                                  entropy,
                                                                                                                  self.id2thread2[
                                                                                                                      x].users_count(),
                                                                                                                  ttt,
                                                                                                                  isOpen) + '-' * 40 + '\n')
            file.write('Leader is {0}: "{1}"\n'.format(x, text))
            file.write(
                'thread\tleading doc\titem#\titem ID\tuser\ttimestamp\tnearest ID\tdistance\titem text\titem text(original)\n')
            c = 1
            for item in self.id2thread2[x].idlist:
                i = self.doc_indices[item]
                text1 = self.text_data[i]
                text2 = self.text_metadata[item]['text']
                user = self.text_metadata[item]['user']
                timestamp = self.text_metadata[item]['created_at']
                nearID = self.id2thread2[x].document_contents[item][0]
                nearestDist = self.id2thread2[x].document_contents[item][1]
                file.write(
                    '{0}\t{1}\t{2}\t{3}\t{4}\t{7}\t{8}\t"{5}"\t"{6}"\t"{9}"\n'.format(thr, x, c, item, user, timestamp,
                                                                                      text1, text2, nearID,
                                                                                      nearestDist))
                c += 1
            if self.id2thread2[x].is_open():
                thr += 1

            if thr > max_threads:
                break

        if file != None:
            file.close()
            # self.session.logger.exit('dumpThreads')


class MasterClusteringProcess(ServiceInterface):
    id2document = {}

    def __init__(self, name, session=None):
        agentname=MasterClusteringAgent.__module__ + '.' + MasterClusteringAgent.__name__
        print(agentname)
        self.id2document = {}
        ServiceInterface.__init__(self, name, agentname, session)

    def match_to_cluster(self, id, point, compare_to):
        self.request(MAP_TO_THREAD, id, point, compare_to)

    def add(self, id, document):
        self.id2document[id] = document
        self.request(ADD, id, document)

    def remove(self, id):
        self.id2document.pop(id)
        self.request(EXPIRE, id)

    def printThreads(self, filename, max_threads):
        self.request(DUMP_THREADS, filename, max_threads)


