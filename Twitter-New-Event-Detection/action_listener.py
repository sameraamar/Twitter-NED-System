from pymongo import MongoClient
import pymongo
from sklearn.feature_extraction.text import CountVectorizer
from simple_twitter_parser import preprocess, clean_spaces

import numpy as np
import time, json

class Listener:
    session = None

    def __init__(self, session):
        self.session = session

    def act(self, data):
        # do nothing and continue
        return True


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

    def init(self, filename):
        super(self.__class__, self).__init__()
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

        skip = 0
        done = False
        while not done:
            cursor = self.dbcoll.find({'status' : 'Loaded'})
            cursor.sort("_id", pymongo.ASCENDING)
            cursor.skip(skip)
            print('Query on mongodb performed (skipped {0} documents)'.format(skip))
            printme = True
            try:
                for item in cursor:
                    if (printme):
                        self.session.logger.info('New item is: {0}.'.format(item))
                        printme = False

                    skip += 1
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
                    if skip % 500 == 0:
                        self.session.logger.fine('Cursor index {0}. Processed successfuly {1}'.format(skip, self.session.increment_counter()))

                    previous = data

                    # publish to listeners
                    self.session.increment_counter()
                    flag = self.publish(data)

                    if not flag:
                        break

                done = True
            except pymongo.errors.CursorNotFound as e: #pymongo.errors.OperationFailure as e:
                #t1 = time.ctime(basetime)
                t2 = time.ctime(time.time())
                self.session.logger.error(' ***** Failed at {0}:).'.format(t2))
                self.session.logger.error(' \tPrevious item was: {0} (processing time was {1} seconds).'.format(previous, time.time() - lasttime))
                self.session.logger.error(' \tError: {0}'.format( e ))
                skip -= 1
                #if not (msg.startswith("cursor id") and msg.endswith("not valid at server")):
                #    raise

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

    def act(self, data):
        self.session.logger.entry("TwitterTextListener.act")

        itemText_tmp = data['text']
        itemText = ' '.join(self.process(itemText_tmp))

        if len(itemText) == 0:
            self.session.logger.exit("TwitterTextListener.act")

            return True

        metadata = {}
        ID = data['id_str']

        metadata['retweet'] = (data.get('retweet', None) != None)

        metadata['user'] = data['user']['screen_name']
        metadata['timestamp'] = data['timestamp']

        metadata['text'] = data['text'].replace('\t', ' ').replace('\n', '. ')
        """
        self.text_data.append( itemText )
        self.id_list.append ( ID )

        index = len(self.text_data)-1
        self.text_metadata[ ID ] = metadata
        self.doc_indices[ ID ] = index

        if index == 1:
            self.session.logger.info("First tweet: {}".format(metadata))
        """

        base = time.time()
        index = self.lshmodel.addDocument(ID, itemText, metadata)

        #self.times.append(time.time() - base)

        #fraction = 100.0 * index / self.max_documents
        #if int(fraction) == fraction:
        #    self.session.logger.info(
        #        "Total processed {0} (AHT: {2:.2f}(s)). Word vector dimention is {1}".format(self.lshmodel.processed,
        #                                                                                     self.lshmodel.getDimension(),
        #                                                                                     np.average(self.times)))
        #    self.times = []

        if index + 1 == self.max_documents:
            before = time.time()
            self.session.logger.info("Last tweet: {}".format(metadata))
            self.session.logger.info('running LSH on {} documents'.format(index + 1))

            # self.lshmodel.run(self.text_data, self.id_list, self.text_metadata, self.doc_indices)
            x = time.time() - before
            x = (int(x / 60), int(x) % 60)

            self.session.logger.info('Time for running the LSH model was: {0} min and {1} sec'.format(x[0], x[1]))
            self.session.logger.exit("TwitterTextListener.act")
            return False

        if index % 100 == 0:
            self.session.logger.debug('Loaded {} documents'.format(index))

        self.session.logger.exit("TwitterTextListener.act")
        return True

    def process(self, text):
        return clean_spaces(text)
        #preprocess(text, return_text=True, numbers=True, mentions=False, stop_words=False, hashtag=True)
