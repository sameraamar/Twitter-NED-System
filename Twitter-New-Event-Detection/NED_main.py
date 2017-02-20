# %%

import psutil
import os
import sys
import os.path
import time
from action_listener import MongoDBStreamer, TwitterTextListener, TextGZipStreameSet
from session import Session, MongoDBHandler, TextFileHandler
from simplelogger import simplelogger
from NED import NED_LSH_model
import json
import datetime
from session import human_time

def memory_usage_psutil():
    # return the memory usage in MB
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss  # [0] / float(2 ** 20)
    return mem


def init_mongodb(k, maxB, tables, threshold, max_docs, page, recent_documents,
                 multiprocess, num_processes, dimension_jumps,
                 dimension, max_thread_delta_time,
                 tfidf_mode = True, tracker=False, profiling_idx=0):
    print('Running LSH with {0} tweets ..... '.format(max_docs), end='')


    # %%
    session = Session(tracker_on=tracker)

    session.generate_temp_folder('LSH', suffix='_'+str(max_docs))

    log_filename = session.get_temp_folder() + '/log_{0:07d}_docs.log'.format(max_docs)

    session.init_logger(filename=log_filename, std_level=simplelogger.INFO, file_level=simplelogger.DEBUG, profiling=False)
    # logger.init(filename=log_filename, std_level=simplelogger.INFO, file_level=simplelogger.DEBUG, profiling=False)

    # %%
    lshmodel = NED_LSH_model()
    lshmodel.init(session, k, tables, max_bucket_size=maxB, dimension=dimension, threshold=threshold,
                  multiprocess=multiprocess, num_processes=num_processes, dimension_jumps=dimension_jumps,
                  recent_documents=recent_documents, tfidf_mode = tfidf_mode, max_thread_delta_time=max_thread_delta_time,
                  profiling_idx=profiling_idx)

    session.setLSHModel(lshmodel)

    return session, lshmodel


def execute(session, lshmodel, offset, max_docs, host, port, db, collection, max_threads, folder='.', filenames=None):
    session.logger.entry("main.execute")
    base = time.time()
    # streamer = TextStreamer(logger)
    # streamer.init('C:\data\_Personal\DataScientist\datasets\Italy1.json')

    if filenames:
        streamer = TextGZipStreameSet(session, folder, filenames, offset=offset)
    else:
        streamer = MongoDBStreamer(session)
        streamer.init(host, port, db, collection, offset=offset)

    listener = TwitterTextListener(session)
    listener.init(lshmodel, max_docs)

    streamer.register(listener)
    streamer.start()

    nn = listener.lshmodel.processed
    delta = listener.lshmodel.last_timestamp - listener.lshmodel.first_timestamp
    exec_time = time.time() - base
    session.logger.info('Processed {0} text documents in {2} (time delta between documents: {1}) '.format(nn, human_time(seconds=delta), human_time(seconds=exec_time)))

    # %%
    threads_filename = session.get_temp_folder() + '/threads_{0:07d}_docs.txt'.format(max_docs)

    #lshmodel.dumpThreads(threads_filename.replace('.txt', '1.txt'), max_threads)
    #lshmodel.dumpThreads2(threads_filename.replace('.txt', '2.txt'), max_threads)
    lshmodel.dumpThreads(threads_filename.replace('.txt', '3.txt'), max_threads)

    session.logger.exit("main.execute")

    return lshmodel

def printInfo(session, lshmodel, measured_time, count, max_docs):
    session.logger.info('print profiling!')

    temp = session.get_temp_folder()
    session.logger.profiling_dump(human_time, path=temp, avg_base=count)
    msg = 'done with {0:.2f} seconds (= {1:.2f} minutes).'.format(measured_time, measured_time / 60)
    print(msg)
    session.logger.info(msg)


    # %%
    session.logger.info('Done ({0} documents processed, {1}).'.format(max_docs, session.get_temp_folder()))
    session.logger.close()

    # input('Round {} is done. Press Enter...'.format(r))

    return lshmodel


def mymain(num_tables=4, num_processes=8):
    k = 13
    maxB = 100  # should be less than 0.5 of max_docs/(2^k)

    NUM_PROCESS = num_processes
    multiprocess = NUM_PROCESS>0

    dimension = 50000
    DIMENSION_JUMPS = 5000

    tables = num_tables
    threshold = 0.5
    # %%
    max_threads = 2000
    max_docs = 10000
    recent_documents = 0
    max_thread_delta_time = 3600  # 1 hour delta maximum

    tfidf_mode = True

    # %%

    # mongodb
    host = 'localhost' #'192.168.1.102'
    port = 27017
    dbname = 'events2012'  # 'petrovic'
    dbcoll = 'posts'  # 'relevance_judgments'
    #dbname = 'test'
    #dbcoll = 'test'

    folder = 'C:\\data\\events_db\\petrovic'
    filenames = [   'petrovic_00000000.gz',
                    'petrovic_00500000.gz',
                    'petrovic_01000000.gz',
                    'petrovic_01500000.gz',
                    'petrovic_02000000.gz',
                    'petrovic_02500000.gz',
                    'petrovic_03000000.gz',
                    'petrovic_03500000.gz',
                    'petrovic_04000000.gz',
                    'petrovic_04500000.gz',
                    'petrovic_05000000.gz',
                    'petrovic_05500000.gz',
                    'petrovic_06000000.gz',
                    'petrovic_06500000.gz',
                    'petrovic_07000000.gz',
                    'petrovic_07500000.gz',
                    'petrovic_08000000.gz',
                    'petrovic_08500000.gz',
                    'petrovic_09000000.gz',
                    'petrovic_09500000.gz',
                    'petrovic_10000000.gz',
                    'petrovic_10500000.gz',
                    'petrovic_11000000.gz',
                    'petrovic_11500000.gz',
                    'petrovic_12000000.gz',
                    'petrovic_12500000.gz',
                    'petrovic_13000000.gz',
                    'petrovic_13500000.gz',
                    'petrovic_14000000.gz',
                    'petrovic_14500000.gz',
                    'petrovic_15000000.gz',
                    'petrovic_15500000.gz',
                    'petrovic_16000000.gz',
                    'petrovic_16500000.gz',
                    'petrovic_17000000.gz',
                    'petrovic_17500000.gz',
                    'petrovic_18000000.gz',
                    'petrovic_18500000.gz',
                    'petrovic_19000000.gz',
                    'petrovic_19500000.gz',
                    'petrovic_20000000.gz',
                    'petrovic_20500000.gz',
                    'petrovic_21000000.gz',
                    'petrovic_21500000.gz',
                    'petrovic_22000000.gz',
                    'petrovic_22500000.gz',
                    'petrovic_23000000.gz',
                    'petrovic_23500000.gz',
                    'petrovic_24000000.gz',
                    'petrovic_24500000.gz',
                    'petrovic_25000000.gz',
                    'petrovic_25500000.gz',
                    'petrovic_26000000.gz',
                    'petrovic_26500000.gz',
                    'petrovic_27000000.gz',
                    'petrovic_27500000.gz',
                    'petrovic_28000000.gz',
                    'petrovic_28500000.gz',
                    'petrovic_29000000.gz',
                    'petrovic_29500000.gz' ]

    min_rounds = 0
    max_rounds = 10
    profiling_idx = 100000000
    offset = 0

    tracker = False

    lsh = {
        'title': 'LSH parameters',
        'k': {'value': k, 'label': 'Hyperplanes'},
        'maxB': {'value': maxB, 'label': 'Max bucket size'},
        'tables': {'value': tables, 'label': 'Number of Hashtables'},
        'tfidf': {'value': tfidf_mode, 'label': 'TFIDF?'}
    }

    thread = {
        'title': 'Thread',
        'max_docs': {'value': max_docs, 'label': 'Max Input Documents'},
        'offset': {'value': offset, 'label': 'Input Documents Offset'},
        'recent_documents': {'value': recent_documents, 'label': 'Search Recent Documents'},
        'max_threads': {'value': max_threads, 'label': 'Max Threads'},
        'threshold': {'value': threshold, 'label': 'Threshold'},
        'max_thread_delta_time': {'value': max_thread_delta_time, 'label': 'Thread Closes after? (sec)'}
    }

    mongodb = {
        'title': 'Mongo DB',
        'dbname': {'value': dbname, 'label': 'MongoDB-DB Name'},
        'dbcoll': {'value': dbcoll, 'label': 'MongoDB-Collection'},
        'dbhost': {'value': host, 'label': 'Mongo DB Host'},
        'dbport': {'value': port, 'label': 'Mongo DB Port'}
    }

    parameters = {
        'lsh': lsh,
        'thread': thread,
        'mongodb': mongodb,
        'tracker': {'value': tracker, 'label': 'Tracker Flag (T/F)'}
    }


    if len(sys.argv) > 1:
        max_docs = int(sys.argv[1])

    if len(sys.argv) > 2:
        min_rounds = int(sys.argv[2])
        max_rounds = int(sys.argv[3])

    session, lshmodel = init_mongodb(k, maxB, tables, threshold, max_docs, offset, recent_documents=recent_documents,
                                     multiprocess=multiprocess, num_processes=NUM_PROCESS, dimension_jumps=DIMENSION_JUMPS,
                                     dimension=dimension, max_thread_delta_time=max_thread_delta_time, tfidf_mode = tfidf_mode,
                                     tracker=tracker, profiling_idx=profiling_idx)
    try:
        session.logger.info(json.dumps(parameters, indent=4, sort_keys=True))

        #mongodb_url= 'mongodb://{0}:{1}'.format(host, port)
        #dbname = 'output'
        #m = MongoDBHandler(session, mongodb_url, dbname)
        #session.init_output(m)

        m = TextFileHandler(session)
        session.init_output(m)

        preformance_file = session.get_temp_folder() + '/../performance.txt'

        newFile = True
        if os.path.isfile(preformance_file ):
            newFile = False
        file = open(preformance_file, 'a')

        if newFile:
            file.write('max_docs\tseconds\tminutes\ttime\tprocesses\ttables\n')

        starttime = time.time()
        execute(session, lshmodel, offset, max_docs, host, port, dbname, dbcoll, max_threads, folder=folder, filenames=filenames)
        measured_time = time.time() - starttime
        #usage_psutil = memory_usage_psutil()

        strtime = datetime.datetime.fromtimestamp( starttime ).strftime('%Y-%m-%d %H:%M:%S')
        file.write('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n'.format(max_docs, measured_time, measured_time / 60, strtime, NUM_PROCESS, tables))
        file.close()

        printInfo(session, lshmodel, measured_time, max_docs-profiling_idx, max_docs)
    except Exception as e:
        raise e
    finally:
        lshmodel.lsh.finish()
        session.finish()


if __name__ == '__main__':
    from math import pow
    for pow2 in range(0, 4, 1):
        num_processes = int(pow(2, pow2))
        for tables in range(1, 64, 10):
            if tables >= num_processes:
                print('processes: ', num_processes, ' tables: ', tables)
                mymain(tables, num_processes)
