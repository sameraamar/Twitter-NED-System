from simplelogger import simplelogger
from math import log
import numpy as np




class TweetThread:
    session = None
    thread_id = None
    thread_doc = None
    score = 0
    document_contents = None
    idlist = list()
    counter = {}
    users = set()
    count_all = 0

    all_words = None
    min_timestamp = None
    max_timestamp = None
    max_time_delta = 0

    saved = False
    

    def __init__(self, session, thread_id, word_counts, user, timestamp, max_time_delta):
        self.session = session
        self.thread_id = thread_id
        self.thread_doc = word_counts
        self.reset()
        self.min_timestamp = timestamp
        self.max_timestamp = timestamp
        self.max_time_delta = max_time_delta

        self.saved = False
        
        self.append(thread_id, word_counts, user, timestamp, None, None)

    def reset(self):
        self.document_contents = {}
        self.idlist = list()
        self.counter = {}
        self.all_words = None
        self.users = set()
        self.count_all = 0

    def append1(self, ID, word_counts, user, timestamp, nearID, nearestDist):

        self.session.logger.entry('tweet_thread.append')
        self.document_contents[ID] = (None, nearID, nearestDist)
        self.idlist.append(ID)

        self.users.add(user)

        if self.min_timestamp > timestamp:
            self.min_timestamp = timestamp

        if self.max_timestamp < timestamp:
            self.max_timestamp = timestamp

        if self.size() == 1:
            self.all_words = np.zeros_like(word_counts)
            #np.copyto(self.all_words, word_counts)

        self.all_words = self.all_words + word_counts

        self.session.logger.exit('tweet_thread.append')

    def dump(self, text_metadata):
        if not self.saved :
            #already saved
            return

        self.session.logger.entry('tweet_thread.dump')

        self.session.logger.info('Going to dump this thread: lead: '.format( self.thread_id, ': ', end=''))

        entr = self.entropy()
        entries = []
        msg = list()
        for ID in self.idlist:
            nearID, nearestDist = self.document_contents[ID]
            entry = { 'nearest': nearID,
                      'nearestDist': nearestDist,
                      'text' : text_metadata[ID]['text'],
                      'user' : text_metadata[ID]['user'] }
            entries.append(entry)
            msg.append(ID)
        self.session.logger.info( ', '.join(msg) )

        doc = {}
        doc['thread_id'] = self.thread_id
        doc['thread_text'] = text_metadata[self.thread_id]

        doc['list'] = entries
        doc['entropy'] = entr

        self.session.output.write_thread( thread_id=self.thread_id, thread_details=doc )


        self.session.logger.exit('tweet_thread.dump')

    def append(self, ID, word_counts, user, timestamp, nearID, nearestDist):
        self.session.logger.entry('tweet_thread.append')

        self.document_contents[ID] = (nearID, nearestDist)
        self.idlist.append(ID)
        nonzeros = np.nonzero(word_counts)
        for i in nonzeros[1]:
            c = word_counts[0,i]
            self.counter[i] = self.counter.get(i, 0) + c

            if(self.counter[i] != int(self.counter[i])):
                print(ID, word_counts)
                assert(False)

            self.count_all += c
        self.users.add(user) 
        
        if self.min_timestamp > timestamp:
            self.min_timestamp = timestamp
            
        if self.max_timestamp < timestamp:
            self.max_timestamp = timestamp

        self.session.logger.exit('tweet_thread.append')

    def is_open(self, current_timestamp):
        # 1 hour max time
        return current_timestamp-self.min_timestamp < self.max_time_delta;

    def users_count(self):
        return len(self.users)

    def size(self):
        return len(self.idlist)

    def entropy1(self):
        N = np.sum(self.all_words)
        temp = self.all_words / N

        temp_log = np.log(temp, 10)
        temp = temp * temp_log
        segma = np.sum(temp)

        return segma

    def entropy(self):
        segma = 0
        for i in self.counter:
            d = float(self.counter[i]) / self.count_all
            segma -= d * log(d)
        return segma

    def thread_time(self):
        return (self.max_timestamp-self.min_timestamp)
