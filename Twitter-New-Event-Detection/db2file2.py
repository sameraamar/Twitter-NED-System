from action_listener import MongoDBStreamer, Listener
from session import Session
from simplelogger import simplelogger

class TwitterSave2TextFileListener(Listener):
    filepath = None
    maxperfile = 0
    index = 0
    
    def init(self, filepath, maxperfile=100000):
        self.maxperfile = maxperfile
        self.filepath = filepath
        self.index = 0

    def act(self, data):
        self.session.logger.entry("TwitterSave2TextFileListener.act")

        itemText_tmp = data['text']
        itemText = ' '.join(self.process(itemText_tmp))

        if len(itemText) == 0:
            self.session.logger.exit("TwitterSave2TextFileListener.act")

            return True

        metadata = {}
        ID = data['id_str']

        metadata['retweet'] = (data.get('retweet', None) != None)

        metadata['user'] = data['user']['screen_name']
        metadata['timestamp'] = data['timestamp']

        metadata['text'] = data['text'].replace('\t', ' ').replace('\n', '. ')

        self.index += 1
        if self.index % 100 == 0:
            self.session.logger.debug('Loaded {} documents'.format(self.index))

        self.session.logger.exit("TwitterSave2TextFileListener.act")
        return True

    def process(self, text):
        return text #clean_spaces(text)
        #preprocess(text, return_text=True, numbers=True, mentions=False, stop_words=False, hashtag=True)



host = 'localhost'  # '192.168.1.102'
port = 27017
dbname = 'events2012'  # 'petrovic'
dbcoll = 'posts'  # 'relevance_judgments'

session = Session(tracker_on=False)
log_filename = session.temp_folder(suffix='_DB2TXT') + '/audit.log'
session.init_logger(filename=log_filename, std_level=simplelogger.INFO, file_level=simplelogger.DEBUG, profiling=True)

streamer = MongoDBStreamer(session)
streamer.init(host, port, dbname, dbcoll, offset=0)

listener = TwitterSave2TextFileListener(session)
listener.init('c:/temp')

streamer.register(listener)
streamer.start()
