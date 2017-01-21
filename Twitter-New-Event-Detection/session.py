
from simplelogger import simplelogger
import datetime, time

from tempfile import gettempdir
import os.path as path
from os import makedirs
import uuid


def varname(var):
  frame = inspect.currentframe()
  var_id = id(var)

  for k, v in list(locals().iteritems()):
      if v is a:
          a_as_str = k

  for name in frame.f_back.f_locals.keys():
    try:
      if id(eval(name)) == var_id:
        return(name)
    except:
      pass

def get_variable_name(*variable):
    '''gets string of variable name
    inputs
        variable (str)
    returns
        string
    '''
    if len(variable) != 1:
        raise Exception('len of variables inputed must be 1')
    try:
        ret = [k for k, v in locals().items() if id(v) == id(variable[0])]
        ret = ret[0]
    except:
        ret = [k for k, v in globals().items() if id(v) == id(variable[0])]
        ret = ret[0]

    return ret

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
    tracker_on = False
    cb = None
    #tr = None

    def __init__(self, tracker_on=False):
        self.unique_id = str(datetime.datetime.utcnow()) #datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        self.tracker_on = tracker_on
        if self.tracker_on:
            from pympler import tracker
            from pympler import refbrowser
            import inspect

            self.cb = refbrowser.FileBrowser(self, str_func=output_function)

            #self.tr = tracker.SummaryTracker()
            #self.tr.print_diff()
        return

    def temp_folder(self):
        if self._temp_folder == None:
            temp = "".join(x for x in self.unique_id if x.isalnum())
            folder = path.join(gettempdir(), 'LSH', temp)
            self._temp_folder = folder

            if not path.exists(folder):
                makedirs(folder)

        return self._temp_folder

    def init_logger(self, filename, std_level=simplelogger.INFO, file_level=simplelogger.INFO, profiling=True):
        self.logger = simplelogger()
        self.logger.init(filename=filename, std_level=std_level, file_level=file_level, profiling=profiling)

    def init_output(self, handler):
        self.output = handler

    def increment_counter(self):
        self.processed_ += 1
        n = 1000
        if (self.processed_ > 0) and (self.processed_ % n == 0):
            #filename = uniqueTempFileName(self._temp_folder)
            page = int(self.processed_ / n)
            threads_filename = '{0}/threads_{1:03d}.txt'.format(self._temp_folder , page)
            self.logger.info('Processed {0}. Output {1}'.format( self.processed_ , threads_filename))
            self.lshmodel.dumpThreads3(threads_filename, max_threads=2000)

        if self.tracker_on and (self.processed_ % 500 == 0) :
            self.lshmodel.myprint()

            #self.cb.print_tree('{0}/print_tree_{1:06d}.log'.format(self._temp_folder , self.processed_))
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

    def close(self):
        return
