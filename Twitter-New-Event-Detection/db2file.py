from action_listener import MongoDBStreamer, Listener
from session import Session
from simplelogger import simplelogger
import json, codecs
import gzip
from os import path

class TwitterSave2TextFileListener(Listener):
    filepath = '.'
    prefix = ''
    maxperfile = 0
    maxtotal = 0
    index = 0
    outfile = None
    filenames = []
    
    def init(self, filepath, prefix, maxperfile=100000, maxtotal=-1):
        self.maxperfile = maxperfile
        self.filepath = filepath
        self.maxtotal = maxtotal
        self.prefix = prefix
        self.index = 0
        self.filenames = []

    def act(self, data):
        self.session.logger.entry("TwitterSave2TextFileListener.act")

        #itemText_tmp = data['text']
        #itemText = ' '.join(self.process(itemText_tmp))

        #if len(itemText) == 0:
        #    self.session.logger.exit("TwitterSave2TextFileListener.act")
        #
        #    return True

        """
        metadata = {}
        ID = data['id_str']

        metadata['retweet'] = (data.get('retweet', None) != None)

        metadata['user'] = data['user'].get('screen_name', None)
        if metadata['user']
        metadata['user'] = data['user']['screen_name']
        metadata['timestamp'] = data['timestamp']

        metadata['text'] = data['text'].replace('\t', ' ').replace('\n', '. ')
        """

        #self.session.logger.info(metadata['text'])
        if self.index % 1000 == 0:
            self.session.logger.debug('Loaded {} documents'.format(self.index))


        if self.index % self.maxperfile == 0:
            self.session.logger.info('Loaded {} documents'.format(self.index))
            if self.outfile!=None:
                self.outfile.close()

            fname = '{0}_{1:08d}.gz'.format(self.prefix, self.index)
            fullfilename = path.join(self.filepath, fname)
            self.outfile = gzip.open(fullfilename, 'wt', encoding='utf-8')
            self.filenames.append(fname)
            #self.outfile = codecs.open('data.json', 'w', 'utf8')

        self.index += 1
        self.outfile.writelines(json.dumps(data, sort_keys = True, ensure_ascii=False))
        #self.outfile.write('\n')

        #print(self.index, metadata['text'] )

        self.session.logger.exit("TwitterSave2TextFileListener.act")

        to_continue = self.maxtotal<0 or self.index < self.maxtotal

        if not to_continue:
            self.outfile.close()

        return to_continue


    def process(self, text):
        return text #clean_spaces(text)
        #preprocess(text, return_text=True, numbers=True, mentions=False, stop_words=False, hashtag=True)

port = 27017
host = '192.168.1.102'
#host = 'localhost'
#dbname = 'events2012'
dbname = 'petrovic' #'events2012'
#dbcoll = 'posts'
dbcoll = 'relevance_judgments'

session = Session(tracker_on=False)
folder = session.generate_temp_folder('DB2TXT')
log_filename = path.join(folder , 'audit.log')
print(log_filename)
session.init_logger(filename=log_filename, std_level=simplelogger.DEBUG, file_level=simplelogger.DEBUG, profiling=True)

streamer = MongoDBStreamer(session)
streamer.init(host, port, dbname, dbcoll, offset=0)

listener = TwitterSave2TextFileListener(session)
listener.init(folder, 'relevance_judgments', maxperfile=100000, maxtotal=-1)

streamer.register(listener)
streamer.start()

session.logger.info(listener.filepath)
session.logger.info(str(listener.filenames))

print('Check integrity')

listener.outfile.close()

for fname in listener.filenames:
    file = gzip.open(listener.filepath + '/' + fname, 'rt', encoding='utf-8')
    count = 0
    print('run counter')
    for line in file:
        count += 1
    print('closing file')

    file.close()

    session.logger.info('File {0} : {1} entries'.format(fname, count))


print('Done.')
