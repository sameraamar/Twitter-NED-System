from simplelogger import simplelogger
from math import log10
import numpy as np
from session import human_time
import json

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

    def dump(self, output, id2document):
        if self.saved :
            #already saved
            return

        #self.session.logger.entry('tweet_thread.dump')

        #self.session.logger.info('Going to dump this thread: lead: '.format( self.thread_id, ': ', end=''))
        self.saved = True

        entr = self.entropy()

        if entr < 1.5:
            return

        entries = []
        #msg = list()
        for ID in self.idlist:
            nearID, nearestDist = self.document_contents[ID]
            data = id2document[ID].metadata
            entry = { 'ID' : ID,
                      'nearest': nearID,
                      'nearestDist': nearestDist,
                      'text' : data['text'],
                      'user' : data['user'],
                      'timestamp' : data['timestamp'],
                      'created_at': data['created_at']}
            entries.append(entry)
            #msg.append(ID)
        #self.session.logger.info( ', '.join(msg) )

        doc = {}
        doc['thread_id'] = self.thread_id
        doc['thread_text'] = id2document[self.thread_id].metadata['text']

        doc['size'] = len(self.idlist)
        doc['entries'] = entries
        doc['entropy'] = entr
        doc['users'] = self.users_count()
        doc['period'] = human_time(seconds=self.thread_time())

        output.write_thread( thread_id=self.thread_id, thread_details=doc )

        #text = json.dumps({'id': self.thread_id, 'thread': doc}, indent=4, sort_keys=True)
        #output.write(text)
        #output.write('\n')

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

    def can_add(self, new_timestamp):
        return new_timestamp - self.min_timestamp < self.max_time_delta;

    def is_open2(self):
        # 1 hour max time
        return self.max_timestamp - self.min_timestamp < self.max_time_delta;

    def too_old2(self, ts, strict_mode=False):
        if strict_mode:
            compare = self.max_timestamp
        else:
            compare = self.min_timestamp
        d = ts - compare

        return d > self.max_time_delta

    def users_count(self):
        return len(self.users)

    def size(self):
        return len(self.idlist)

    def entropy1(self):
        N = np.sum(self.all_words)
        temp = self.all_words / N

        temp_log = np.log10(temp, 10)
        temp = temp * temp_log
        segma = np.sum(temp)

        return segma

    def entropy(self):
        segma = 0
        for i in self.counter:
            d = float(self.counter[i]) / self.count_all
            segma -= d * log10(d)
        return segma

    def thread_time(self):
        return (self.max_timestamp-self.min_timestamp)
