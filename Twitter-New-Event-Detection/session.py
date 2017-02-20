
from simplelogger import simplelogger
import datetime, time



from tempfile import gettempdir
import os.path as path
from os import makedirs
import uuid
import codecs, json

refbrowser_on = False

import datetime
import math

def human_time(*args, **kwargs):
    '''
    human_time([days[, seconds[, microseconds[, milliseconds[, minutes[, hours[, weeks]]]]]]])
    All arguments are optional and default to 0. Arguments may be ints, longs, or floats, and may be positive or negative.

    arguments are following the format of datetime.timedelta() function
    :return: string
    '''
    secs  = float(datetime.timedelta(*args, **kwargs).total_seconds())
    units = [("day", 86400), ("hour", 3600), ("minute", 60), ("second", 1)]
    parts = []
    for unit, mul in units:
        if secs / mul >= 1 or mul == 1:
            if mul > 1:
                n = int(math.floor(secs / mul))
                secs -= n * mul
            else:
                n = secs if secs != int(secs) else int(secs)
            parts.append("%s %s%s" % (n, unit, "" if n == 1 else "s"))
    return ", ".join(parts)

def output_function(o):
    txt1 = txt2 = ''
    if isinstance(o, dict) or isinstance(o, list):
        txt1 = str(len(o))
        #txt2 = get_variable_name(o)

    return ' '.join([str(type(o)) , txt1, txt2])


def uniqueTempFileName(folder):
    if not path.exists(folder):
        makedirs(folder)
    filename = path.join(folder, str(uuid.uuid4()) + '.dat')
    return filename


def uniqueID():
    return str(uuid.uuid4())

class Session:
    unique_id = None
    _temp_folder = None
    logger = None
    output = None
    processed_ = 0
    lshmodel = None

    #profiling
    profiling_idx = 0
    tracker_on = False
    cb = None
    #tr = None

    def __init__(self, tracker_on=False):
        self.unique_id = str(datetime.datetime.utcnow()) #datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        self.tracker_on = tracker_on

        global refbrowser_on
        if refbrowser_on:
            from pympler import tracker
            from pympler import refbrowser
            import inspect

            self.cb = refbrowser.FileBrowser(self, str_func=output_function)

            #self.tr = tracker.SummaryTracker()
            #self.tr.print_diff()
        return

    def finish(self):
        if self.output != None:
            self.output.close()

    def generate_temp_folder(self, parentfolder, prefix='', suffix=''):
        if self._temp_folder == None:
            temp = "".join(x for x in self.unique_id if x.isalnum())
            folder = path.join(gettempdir(), parentfolder, prefix + temp + suffix)
            self._temp_folder = folder

            if not path.exists(folder):
                makedirs(folder)

        return self._temp_folder

    def get_temp_folder(self):
        if self._temp_folder == None:
            raise Exception('no temp folder created. call session.generate_temp_folder() first')

        return self._temp_folder

    def init_logger(self, filename, std_level=simplelogger.INFO, file_level=simplelogger.INFO, profiling=True):
        self.logger = simplelogger()
        self.logger.init(filename=filename, std_level=std_level, file_level=file_level, profiling=profiling)

    def init_output(self, handler):
        self.output = handler

    def increment_counter(self):
        self.processed_ += 1

        if self.tracker_on and (self.processed_ % 500 == 0) :
            self.lshmodel.myprint()

            global refbrowser_on
            if refbrowser_on:
                self.cb.print_tree('{0}/print_tree_{1:06d}.log'.format(self._temp_folder , self.processed_))
                #self.tr.print_diff()

        return self.processed_

    def setLSHModel(self, lshmodel):
        self.lshmodel = lshmodel

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

def update(bulk):
    inserts = 0
    duplicates = 0

    try:
        results = bulk.execute()
        inserts += results['nInserted']
    except BulkWriteError as bwe:
        inserts += bwe.details['nInserted']
        duplicates += len(bwe.details['writeErrors'])

    return inserts, duplicates

class MongoDBHandler:
    db_ = None
    session_ = None

    def __init__(self, session, mongodb_url, dbname):
        client = MongoClient(mongodb_url)
        self.db_ = client[dbname]
        self.session_ = session
        return

    def classify_doc(self, doc_id, object):
        self.db_[self.session_.unique_id].update({'_id' : doc_id}, object, upsert=True)
        return

    """def add_to_thread(self, thread_id, entropy, entry):
        thread = self.session_.unique_id + '_threads'
        cursor = self.db_[thread].find_one({'_id': doc_id})

        doc = {}
        doc['list'] = []
        for d in cursor:
            doc = d
        doc['list'].append( entry )
        doc['entropy'] = entropy

        self.db_[thread].update({'_id': doc_id}, doc, upsert=True)
    """
    def write_thread(self, thread_id, thread_details):
        thread = self.session_.unique_id + '_threads'
        cursor = self.db_[thread].find_one({'_id': thread_id})

        # doc = {}
        # doc['list'] = []
        # for d in cursor:
        #    doc = d
        # doc['list'].append( entry )
        # doc['entropy'] = entropy

        # self.db_[thread].update({'_id': doc_id}, doc, upsert=True)
        self.db_[thread].update({'_id': thread_id}, thread_details, upsert=True)

    def close(self):
        return


class TextFileHandler:
    file_ = None
    counter_ = 0
    session_ = None
    dumps_ = 0
    filepattern_ = ''

    def __init__(self, session):
        self.session_ = session
        self.counter_ = 0
        self.dumps_ = 0

        self.filepattern_ = session.get_temp_folder() + '/thread_{0:07d}.txt'
        filename = self.filepattern_.format(self.dumps_)
        self.file_ = codecs.open(filename, mode='+w', encoding='utf-8')

        return

    def classify_doc(self, doc_id, object):
        #text = json.dumps({'id': doc_id, 'thread': object}, indent=4, sort_keys=True)
        #self.file_.write(text)
        return

    """def add_to_thread(self, thread_id, entropy, entry):
        thread = self.session_.unique_id + '_threads'
        cursor = self.db_[thread].find_one({'_id': doc_id})

        doc = {}
        doc['list'] = []
        for d in cursor:
            doc = d
        doc['list'].append( entry )
        doc['entropy'] = entropy

        self.db_[thread].update({'_id': doc_id}, doc, upsert=True)
    """
    def write_thread(self, thread_id, thread_details):
        text = json.dumps({'id': thread_id, 'thread': thread_details}, indent=4, sort_keys=True)
        self.file_.write(text)
        #self.file_.write(text.replace('\n', ' '))
        #self.file_.write('\n')
        self.file_.write('\n**||**\n')

        self.counter_ += 1
        if self.counter_==10:
            self.file_.close()
            self.counter_ = 0
            self.dumps_ += 1
            filename = self.filepattern_.format(self.dumps_)
            self.file_ = codecs.open(filename, mode='+w', encoding='utf-8')

    def close(self):
        print('Closing...')
        self.file_.close()
        return


if __name__ == '__main__':
    print(human_time(seconds=20))
    print(human_time(seconds=60))
    print(human_time(seconds=121))
    print(human_time(seconds=3670))
