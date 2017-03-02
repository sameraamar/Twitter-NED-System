# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 12:51:47 2016

@author: SAMERA
"""

from LSHForest_MP import LSHForest_MP
from scipy.sparse import csr_matrix
import numpy as np
from simple_twitter_parser import preprocess
from session import human_time
import time, codecs
from tweet_threads import TweetThread
import linalg_helper as la
from WordVectorModel import TFIDFModel
from thread_manager import MasterClusteringProcess
import math
#%%

class NED_LSH_model:
    processed = 0
    clustering_process = None

    lsh = None
    tables = 0
    hyper_planes = 0
    max_bucket_size = 0
    session = None
    threshold = 0
    recent_documents = 0
    
    #%%
    #Samerthreads_queue = {}
    #Samerthreads = {}
    #Samertweet2thread = {}
    #Sameralternatives = {}
    #Samertext_data = []
    #Samerdoc_indices = {}
    #Samerid_list = []
    #Samertext_metadata = {}
    first_timestamp = None
    last_timestamp = None
    last_timestamp_tmp = None
    times = []
    max_thread_delta_time = 3600
    #Samerrecent = []

    multiprocess = False

    #
    word2index = {}
    wordindex_next = 0

    #'''
    #Samerrepository = {}
    #count_vect = None
    tfidf = None
    tfidf_mode = True
    profiling_idx = 0

    def getDimension(self):
        return self.lsh.dimSize

    def init(self, session, hyper_planes, tables, max_thread_delta_time, max_bucket_size=50,
             num_processes=None, dimension_jumps=5000,
             dimension=3, threshold=0.5,
             recent_documents=0, tfidf_mode=True, profiling_idx=5000):
        self.session = session
        self.processed = 0
        self.recent_documents = recent_documents
        self.hyper_planes = hyper_planes
        self.tables = tables
        self.max_bucket_size = max_bucket_size
        self.tweet2thread = {}
        self.text_data = []
        self.doc_indices = {}
        self.lsh = None
        self.word2index = {}
        self.wordindex_next = 0
        self.id_list = []
        self.text_metadata = {}
        self.threshold = threshold
        #Samerself.threads = {}
        #Samer.self.threads_queue = {}
        self.alternatives = {}
        self.recent = []
        self.first_timestamp = None
        self.last_timestamp = None
        self.max_thread_delta_time = max_thread_delta_time
        self.profiling_idx = profiling_idx

        self.clustering_process = MasterClusteringProcess("master", session=self.session)
        self.clustering_process.start(self.session.get_temp_folder())

        self.lsh = LSHForest_MP()
        self.lsh.init(session=self.session, dimensionSize=dimension , numberTables=self.tables,
                      num_processes=num_processes, dimension_jumps=dimension_jumps,
                      hyperPlanesNumber=self.hyper_planes, maxBucketSize=self.max_bucket_size)

        self.tfidf = TFIDFModel(initial_dim=dimension)
        self.tfidf_mode = tfidf_mode

    """
    def run(self, text_data, id_list, text_metadata, doc_indices):
        self.session.logger.entry('NED_LSH_model.run')
        self.text_data = text_data
        self.id_list = id_list
        self.text_metadata = text_metadata
        self.doc_indices = doc_indices
        self.threads = {}
        #Samer.self.threads_queue = {}
        self.tweet2thread = {}
        self.first_timestamp = None
        self.last_timestamp = None
        self.recent = []
        
        self.count_vect = CountVectorizer() #stop_words='english')
        self.counts = self.count_vect.fit_transform(text_data)
            
        # update the dimension
        self.dimension = self.counts.shape[1]
        self.rebuild()
            
        nn = len(text_data)
        p = 0.0
        base = before = time.time()
        
#        self.session.logger.debug ('Adding document {0} ({2}) out of {1}'.format(sample, self.counts.shape[0], ID))
#        nearest, nearestDist, comparisons = lshmodel.lsh.add_all(self.id_list, self.counts)
        #self.lsh.add_all(self.doc_indices, self.counts)
        #lshmodel.lsh.add_all2(self.doc_indices, self.counts, self.id_list)


        block = nn / 20
        for sample in range(nn):
            self.myadd(sample)
        self.session.logger.info ('corpus size: {0} '.format(doc.shape[1]))
        self.session.logger.exit('NED_LSH_model.run')
    """
    def generateTFIDF(self, text):
        self.session.logger.entry("generateTFIDF")
        ret = self.tfidf.addDoc(text)
        self.session.logger.exit("generateTFIDF")
        return ret

    def process(self, text):
        return preprocess(text, return_text=True, numbers=True, mentions=False, stop_words=False, hashtag=True)

    def word_vector(self, text):
        self.session.logger.entry("word_vector")
        words = text.split()
        text_counts = {}
        for w in words:
            idx = self.word2index.get(w, None)
            if idx == None:
                idx = self.wordindex_next
                self.wordindex_next += 1
                self.word2index[w] = idx
            text_counts[idx] = text_counts.get(idx, 0) + 1

        data = []
        col = []
        row = []

        for k in text_counts:
            col.append( k )
            row.append(0)
            data.append( text_counts[k] )

        m = csr_matrix((data, (row, col)), shape=(1, self.wordindex_next))

        self.session.logger.exit("word_vector")
        return m

        #>> > row = np.array([0, 0, 1, 2, 2, 2])
        #>> > col = np.array([0, 2, 2, 0, 1, 2])
        #>> > data = np.array([1, 2, 3, 4, 5, 6])
        #>> > csr_matrix((data, (row, col)), shape=(3, 3)).toarray()





    def addDocument(self, ID, itemText, metadata):

        if self.processed == self.profiling_idx:
            print('**************** Turn profiling on......')
            self.session.logger.turn_profiling()

        self.session.logger.entry("NED_LSH_model.addDocument")
        base = time.time()

        index = self._addDocument(ID, itemText, metadata)
        if index < 0:
            self.session.logger.exit("NED_LSH_model.addDocument")
            return index

        self.times.append(time.time() - base)

        if self.last_timestamp_tmp==None:
            self.last_timestamp_tmp = self.last_timestamp

        #n = 5000
        #if (self.processed > 0) and (self.processed % n == 0):
        #    page = int(self.processed / n)
        #
        #    threads_filename = '{0}/threads_GGG_{1:03d}.txt'.format(self.session.get_temp_folder(), page)
        #    self.session.logger.info('Processed {0}. Output {1}'.format(self.processed, threads_filename))
        #    self.dumpThreads(threads_filename, max_threads=2000)

        if (self.processed % 1000 == 0) or (self.last_timestamp - self.last_timestamp_tmp > 300):
            ttt = human_time(seconds=self.last_timestamp - self.first_timestamp)
            #todelete = list()
            #for x in self.threads_queue:
            #    if not self.threads_queue[x].is_open():
            #        todelete.append(x)
            #for x in todelete:
            #    self.threads_queue.pop(x)
            #    self.threads.pop(x)
            #if len(todelete)>0:
            #    self.session.logger.info('Released {0} clusters'.format(len(todelete)))

            msg = 'Processed {0} documents (reported in {3}). (AHT: {2:.5f}(s)). Word vector dimention is {1}'
            msg = msg.format(self.processed,
                    self.getDimension(),
                    np.average(self.times),
                    ttt) #,
                    #len(self.threads_queue),
                    #self.clustering_process.queueSize())

            if self.processed % 2000 == 0:
                msg = '{0} - {1}'.format(msg, self.clustering_process.queueSize())

            self.session.logger.info(msg)
            if self.session.tracker_on:
                self.myprint()

            self.last_timestamp_tmp =self.last_timestamp
            self.times = []

        self.session.logger.exit("NED_LSH_model.addDocument")

        return index

    def _addDocument(self, ID, itemText, metadata):
        self.session.logger.entry("NED_LSH_model._addDocument")

        if not self.tfidf_mode:
            itemText = self.process(itemText)

        if itemText.strip() == '':
            self.session.logger.exit("NED_LSH_model._addDocument")
            return -1

        self.session.logger.entry('_addDocument1')

        self.text_data.append(itemText)
        self.id_list.append(ID)
        index = len(self.text_data) - 1
        #Samerself.text_metadata[ID] = metadata
        self.doc_indices[ID] = index

        if self.tfidf_mode:
            freq, doc = self.generateTFIDF(itemText)
        else:
            # determine if we have new words
            doc = self.word_vector(itemText)
            freq = doc

        doc_point = la.Document(ID, doc, freq, metadata)
        #Samerself.repository[index] = doc_point
        #
        self.processed += 1

        self.session.logger.exit('_addDocument1')
        self.session.logger.entry('_addDocument2')

        #ID = self.id_list[sample]
        #ID = self.id_list[sample]
        #doc = self.counts[sample, :]
        self.clustering_process.add(doc_point.ID, doc_point)
        self.session.logger.exit('_addDocument2')
        self.session.logger.entry('_addDocument3')

        #self.session.logger.debug('Adding document {0} ({2}) out of {1}'.format(sample, self.counts.shape[0], ID))
        compare_to, doc_point = self.lsh.add(doc_point)
        self.session.logger.exit('_addDocument3')
        self.session.logger.entry('_addDocument4')

        data = metadata #Samerself.text_metadata[ID]
        if self.first_timestamp == None:
            self.first_timestamp = data['timestamp']

        if self.last_timestamp == None or self.last_timestamp < data['timestamp']:
            self.last_timestamp = data['timestamp']




        self.clustering_process.match_to_cluster(doc_point.ID, doc_point, compare_to)
        self.session.logger.exit('_addDocument4')

        self.session.logger.exit("NED_LSH_model._addDocument")
        return index

    def myprint(self):
        self.session.logger.debug('*******************************************')
        self.lsh.myprint()

    def dumpThreads_obsolete(self, filename, max_threads):
        #self.clustering_process.printThreads(filename, max_threads)
        return

    def helper_lambda(self, x):
        return '-'.join( [str(self.threads_queue[x].entropy()) , str(self.threads_queue[x].users_count()) ] )
        #return self.threads_queue[x].entropy()
    
    def jsonify(self, max_threads):
        threads = self.jsonify_threads(max_threads)
        tables = {}
        i = 1
        tables['dimension'] = self.lsh.dimSize
        tables['tables'] = []
        for table in self.lsh.hList:
            data = {}
            data['hyperplanes'] = []
            #for hp in table.hyperPlanes:
            #    data['hyperplanes'].append ( hp)
            
            data['buckets'] = []
            data['count'] = len(table.buckets)
            sorted_keys = table.buckets.keys()
            for b in sorted(sorted_keys, reverse=False):
                bucket_data = {}
                bucket_data['hashcode'] = b
                
                temp = table.buckets[b] 
                bucket_data['documents'] = []
                for item in temp:
                    #item['point'] = doc_point
                    #item['hashcode'] = hashcode

                    tmp = {}
                    tmp['ID'] = item['point'].ID
                    tmp['vector'] = str(item['point'].v)
                    tmp['norm'] = item['point'].norm()
                    tmp['text'] = self.text_metadata[tmp['ID']]['text']
                    bucket_data['documents'].append(tmp)

                data['buckets'].append ( bucket_data )
                
            tables['tables'].append(data)
            i += 1

        return threads, tables
        
    def jsonify_threads(self, max_threads):
        data = {}

        data['thread_timeslot'] = self.last_timestamp - self.first_timestamp
        data['threads_count'] = min(max_threads, len(self.threads_queue) )
        data['_list_'] = []
        thr = 1
        for x in sorted(self.threads_queue, key=lambda x: self.helper_lambda(x), reverse=True):
            thread = {}
            threadSize = self.threads_queue[x].size()
            
            #if threadSize<3:    
            #    #not interesting anymore
            #    break
            
            text = self.text_metadata[x]['text'] 
            thread['leader_id'] = x
            thread['leader_text'] = text
            thread['size'] = threadSize
            thread['entropy'] = self.threads_queue[x].entropy()
            thread['users'] = self.threads_queue[x].users_count()
            thread['speed(sec)'] = self.threads_queue[x].thread_time()
            
            array = []
            c = 1
            for item in self.threads_queue[x].idlist:
                i = self.doc_indices[item]
                text1 = self.text_data[i]
                text2 = self.text_metadata[item]['text'] 
                user = self.text_metadata[item]['user']
                nearID = self.threads_queue[x].document_contents[item][0]
                nearestDist = self.threads_queue[x].document_contents[item][1]
                doc = {}
                doc['index'] = c
                doc['id'] = item
                doc['user'] = user
                doc['text_original'] = text2
                doc['text_clean'] = text1
                doc['user'] = user
                doc['nearest'] = nearID
                doc['distance'] = nearestDist
                
                array.append( doc )
                c+=1
            thread['list'] = array
            
            thr += 1
            data['_list_'].append(thread)
            if thr>max_threads:
                break
        
        return data
        

    def finish(self):
        if self.clustering_process.finish():
            n = self.clustering_process.queueSize()
            while n > 1000:
                d = int(1000 * math.log2(n))
                if n%d == 0:
                    print('Queue for process {0} still has {1} requests'.format(self.clustering_process.name, n))
                    time.sleep(0.05)
                n = self.clustering_process.queueSize()

            self.clustering_process.finish_response()

        self.lsh.finish()