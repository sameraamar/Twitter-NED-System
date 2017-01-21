from simplelogger import simplelogger
from math import log
import numpy as np


def for_debug(self, current_timestamp):
    if str(self.thread_id) in [
        '255820067885957120',
        '255820071711174656',
        '255820080229801984',
        '255820080355610625',
        '255820097048940544',
        '255820097132834816',
        '255820105550819332',
        '255820109501849601',
        '255820109740912640',
        '255820155987296257',
        '255820159942529026',
        '255820176577150977',
        '255820197611581440',
        '255820197649346560',
        '255820218528567297',
        '255820223029059584',
        '255820227227553792',
        '255820097048940544', '255820097132834816', '255820042724323329', '255820054942334976', '255820097132834816']:
        print('HHHHHHHHHHH', current_timestamp, self.min_timestamp, self.max_time_delta,
              current_timestamp - self.min_timestamp, current_timestamp - self.min_timestamp < self.max_time_delta)


class TweetThread:
    thread_id = None
    thread_doc = None
    score = 0
    document_contents = None
    idlist = list()
    counter = {}
    users = set()
    count_all = 0
    
    min_timestamp = None
    max_timestamp = None
    max_time_delta = 0
    

    def __init__(self, thread_id, thread_doc, user, timestamp, max_time_delta):
        self.thread_id = thread_id
        self.thread_doc = thread_doc
        self.document_contents = {}
        self.idlist = list()
        self.counter = {}
        self.users = set()
        self.count_all = 0
        self.min_timestamp = timestamp
        self.max_timestamp = timestamp
        self.max_time_delta = max_time_delta
        
        self.append(thread_id, thread_doc, user, timestamp, None, None)


    def append(self, ID, document, user, timestamp, nearID, nearestDist):
        self.document_contents[ID] = (document, nearID, nearestDist)
        self.idlist.append(ID)
        nonzeros = np.nonzero(document)
        for i in nonzeros[1]:
            c = document[0,i]
            self.counter[i] = self.counter.get(i, 0) + c
            self.count_all += c
        self.users.add(user) 
        
        if self.min_timestamp > timestamp:
            self.min_timestamp = timestamp
            
        if self.max_timestamp < timestamp:
            self.max_timestamp = timestamp    

    def is_open(self, current_timestamp):

        #for_debug(self, current_timestamp)

        # 1 hour max time
        return current_timestamp-self.min_timestamp < self.max_time_delta;

    def users_count(self):
        return len(self.users)

    def size(self):
        return len(self.idlist)

    def entropy(self):
        segma = 0
        for i in self.counter:
            d = self.counter[i]/self.count_all
            segma += d * log(d) 
        return (-1) * segma
        
    def thread_time(self):
        return (self.max_timestamp-self.min_timestamp)
