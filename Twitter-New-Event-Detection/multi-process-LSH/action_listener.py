from pymongo import MongoClient
import pymongo
from sklearn.feature_extraction.text import CountVectorizer
from simple_twitter_parser import preprocess, clean_spaces

import numpy as np
import time, json
import gzip

class Listener:
    session = None

    def __init__(self, session):
        self.session = session

    def act(self, data):
        # do nothing and continue
        return True

    def finalize(self):
        return

class Action:
    listeners = []
    session = None

    def __init__(self):
        self.listeners = []
        self.session = None

    def __init__(self, session):
        self.listeners = []
        self.session = session

    def register(self, listener):
        self.listeners.append(listener)

    def publish(self, data):
        self.session.logger.entry("Action.publish")
        proceed = True
        for l in self.listeners:
            proceed = l.act(data) and proceed

        self.session.logger.exit("Action.publish")
        return proceed


class TextFileStreamer(Action):
    source = None

    def __init__(self, session, filename):
        super(self.__class__, self).__init__(session)
        self.source = open(filename, 'r')

    def start(self):
        for line in self.source:
            if line.strip() == '':
                continue

            # json
            data = json.loads(line)
            # convert to text
            created_at = data['created_at']

            itemTimestamp = time.mktime(time.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y"))
            data['timestamp'] = itemTimestamp

            # publish to listeners
            if not self.publish(data):
                break


                # time controller

        self.session.logger.info('TextStreamer is shutting down')


class TextGZipStreameSet(Action):
    source = None
    counter = 0
    filenames = []
    offset = 0
    folder = '.'

    def __init__(self, session, folder, filenames, offset=0):
        super(self.__class__, self).__init__(session)
        self.offset = offset
        self.filenames = filenames
        self.folder = folder
        self.counter = 0


    def start(self):
        stop = False
        self.counter = 0
        for fname in self.filenames:
            self.source = gzip.open(self.folder + '/' + fname, 'rt', encoding='utf-8')

            self.session.logger.info('Loading file {0}'.format(fname))
            for line in self.source:
                if line.strip() == '':
                    continue

                self.counter += 1
                if self.offset > self.counter :
                    if self.counter % int(0.01 * self.offset) == 0:
                        print('skip {0} to {1}'.format(self.counter, self.offset))
                    continue

                # json
                data = json.loads(line)
                # convert to text

                created_at = data.get('created_at', None)
                if created_at is not None:
                    itemTimestamp = time.mktime(time.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y"))
                    data['timestamp'] = itemTimestamp

                # publish to listeners
                if not self.publish(data):
                    stop = True
                    break

            self.source.close()

            if stop:
                break

                    # time controller

        self.session.logger.info('GZip Streamer is shutting down')



class MongoDBStreamer(Action):
    client = None
    dbcoll = None
    offset = 0

    def __init__(self, session):
        super(self.__class__, self).__init__(session)
        self.dbcoll = None
        self.client = None
        self.offset = 0

    def init(self, host, port, dbname, collname, offset):
        self.client = MongoClient(host, int(port))
        db = self.client[dbname]
        self.dbcoll = db[collname]
        self.offset = offset

    def start(self):
        self.session.logger.entry('MongoDBStreamer.start')
        previous = None
        maxDelta = 0

        basetime = lasttime = time.time()

        count = self.offset
        done = False
        three_errors = 3
        while not done:
            #cursor = self.dbcoll.find({'status' : 'Loaded'})
            cursor = self.dbcoll.find({})
            cursor.sort("_id", pymongo.ASCENDING)
            cursor.skip(count)
            print('Query on mongodb performed (skipped {0} documents)'.format(count))
            printme = True
            try:
                for item in cursor:
                    if (printme):
                        self.session.logger.info('New item is: {0}.'.format(item))
                        printme = False

                    count += 1
                    lasttime = time.time()
                    data = item.get('json', None)

                    if data is not None:
                        # data = json.loads(data)
                        # convert to text
                        created_at = data['created_at']

                        itemTimestamp = time.mktime(time.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y"))
                        data['timestamp'] = itemTimestamp

                        if previous != None:
                            delta = data['timestamp'] - previous['timestamp']
                            if delta > maxDelta:
                                maxDelta = delta
                                text = 'Delta between tweets is {4}  ---> {0}: {2}. {1}: {3}'.format(data['id_str'],
                                                                                                     previous['id_str'],
                                                                                                     data['timestamp'],
                                                                                                     previous['timestamp'],
                                                                                                     maxDelta)
                                self.session.logger.error(text)
                        if count % 500 == 0:
                            self.session.logger.fine('Cursor index {0}. Processed successfully {1}'.format(count, self.session.processed_))

                        previous = data
                    #end if

                    # publish to listeners
                    self.session.increment_counter()

                    #to_continue = True
                    #if self.session.increment_counter()<10:
                    to_continue = self.publish(item)
                    if not to_continue:
                        break

                done = True
            except pymongo.errors.CursorNotFound as e: #pymongo.errors.OperationFailure as e:
                #t1 = time.ctime(basetime)
                t2 = time.ctime(time.time())
                self.session.logger.error(' ***** Failed at {0}:).'.format(t2))
                self.session.logger.error(' \tPrevious item was: {0} (processing time was {1} seconds).'.format(previous, time.time() - lasttime))
                self.session.logger.error(' \tError: {0}'.format( e ))
                count -= 1
                #if not (msg.startswith("cursor id") and msg.endswith("not valid at server")):
                #    raise

            except Exception as toe:
                three_errors -= 1
                print('Error in query on DB ', three_errors, str(toe))
                if three_errors == 0:
                    done = True
                    #pass
                raise

        """
        try:
            # for item in self.dbcoll.find({'status' : 'Loaded'}).sort("_id", pymongo.ASCENDING).skip(self.offset):
            for item in self.dbcoll.find().sort("_id", pymongo.ASCENDING).skip(self.offset):
                # json
                counter += 1
                lasttime = time.time()
                data = item.get('json', None)
                if data == None:
                    continue

                # data = json.loads(data)
                # convert to text
                created_at = data['created_at']

                itemTimestamp = time.mktime(time.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y"))
                data['timestamp'] = itemTimestamp

                if previous != None:
                    delta = data['timestamp'] - previous['timestamp']
                    if delta > maxDelta:
                        maxDelta = delta
                        text = 'Delta between tweets is {4}  ---> {0}: {2}. {1}: {3}'.format(data['id_str'],
                                                                                             previous['id_str'],
                                                                                             data['timestamp'],
                                                                                             previous['timestamp'],
                                                                                             maxDelta)
                        self.session.logger.error(text)
                if counter % 500 == 0:
                    print(counter)

                previous = data

                # publish to listeners
                if not self.publish(data):
                    break


                    # time controller
        except Exception as e:
            print(counter)
            t1 = time.ctime( basetime )
            t2 = time.ctime( time.time() )
            print("started at {0}. Failed at {1}. Delay from last query on mongodb is {2} seconds.".format(t1, t2, time.time() - lasttime))
            print(e)
        """

        self.session.logger.info('Database Streamer is shutting down')
        self.session.logger.exit('MongoDBStreamer.start')




class TwitterTextListener(Listener):
    lshmodel = None
    max_documents = 0
    times = []

    def init(self, lshmodel, max_documents):
        self.lshmodel = lshmodel
        self.max_documents = max_documents
        self.times = []

    def act(self, item):
        self.session.logger.entry("TwitterTextListener.act")

        data = item.get('json', None)
        if data == None:
            data = item

        itemText_tmp = data['text']
        itemText = ' '.join(self.process(itemText_tmp))

        if len(itemText) == 0:
            self.session.logger.exit("TwitterTextListener.act")

            return True

        metadata = {}
        ID = data['id_str']

        metadata['retweet'] = (data.get('retweet', None) is not None)

        metadata['user'] = data['user'].get('screen_name', None)
        if metadata['user'] is None:
            metadata['user'] = data['user']['id_str']

        metadata['timestamp'] = data['timestamp']
        metadata['created_at'] = data['created_at']

        metadata['text'] = data['text'].replace('\t', ' ').replace('\n', '. ')

        index = self.lshmodel.addDocument(ID, itemText, metadata)

        if index == -1 or (index + 1 == self.max_documents):
            return False

        if index % 100 == 0:
            self.session.logger.debug('Loaded {} documents'.format(index))

        self.session.logger.exit("TwitterTextListener.act")
        return True

    def process(self, text):
        return clean_spaces(text)
        #preprocess(text, return_text=True, numbers=True, mentions=False, stop_words=False, hashtag=True)
