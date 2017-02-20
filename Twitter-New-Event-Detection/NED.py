# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 12:51:47 2016

@author: SAMERA
"""

from LSH import LSHForest
from LSH import HashtableLSH
from scipy.sparse import csr_matrix
import json
import numpy as np
from simple_twitter_parser import preprocess
from session import human_time

from simplelogger import simplelogger
import time, codecs
from tweet_threads import TweetThread
import linalg_helper as la

from WordVectorModel import TFIDFModel

#%%

class NED_LSH_model:
    processed = 0

    lsh = None
    tables = 0
    hyper_planes = 0
    max_bucket_size = 0
    session = None
    threshold = 0
    recent_documents = 0
    
    #%%
    threads_queue = {}
    threads = {}
    tweet2thread = {}
    alternatives = {}
    text_data = []
    doc_indices = {}
    id_list = []
    text_metadata = {}
    first_timestamp = None
    last_timestamp = None
    last_timestamp_tmp = None
    times = []
    max_thread_delta_time = 3600
    recent = []

    #
    word2index = {}
    wordindex_next = 0

    #'''
    repository = {}
    #count_vect = None
    tfidf = None
    tfidf_mode = True
    profiling_idx = 0

    def getDimension(self):
        return self.lsh.dimSize

    def init(self, session, hyper_planes, tables, max_thread_delta_time, max_bucket_size=50, dimension=3, threshold=0.5,
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
        self.threads = {}
        self.threads_queue = {}
        self.alternatives = {}
        self.recent = []
        self.first_timestamp = None
        self.last_timestamp = None
        self.max_thread_delta_time = max_thread_delta_time
        self.profiling_idx = profiling_idx

        self.lsh = LSHForest()
        self.lsh.init(session=self.session, dimensionSize=dimension , numberTables=self.tables,
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
        self.threads_queue = {}
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
        self.session.logger.entry("NED_LSH_model.addDocument")
        base = time.time()

        if self.processed == self.profiling_idx:
            print('**************** Turn profiling on......')
            self.session.logger.turn_profiling()

        index = self._addDocument(ID, itemText, metadata)
        if index < 0:
            self.session.logger.exit("NED_LSH_model.addDocument")
            return index

        self.times.append(time.time() - base)

        if self.last_timestamp_tmp==None:
            self.last_timestamp_tmp = self.last_timestamp


        n = 5000
        if (self.processed > 0) and (self.processed % n == 0):
            #filename = uniqueTempFileName(self._temp_folder)
            page = int(self.processed / n)

            threads_filename = '{0}/threads_{1:03d}.txt'.format(self.session.get_temp_folder(), page)
            self.session.logger.info('Processed {0}. Output {1}'.format( self.processed , threads_filename))
            self.dumpThreads3(threads_filename, max_threads=2000)


        if self.processed%1000==0 or self.last_timestamp - self.last_timestamp_tmp > 60:
            ttt = human_time(seconds=self.last_timestamp - self.first_timestamp)
            self.session.logger.info(
                "Processed {0} documents (reported in {3}). (AHT: {2:.5f}(s)). Word vector dimention is {1}".format(self.processed,
                                                                                            self.getDimension(),
                                                                                            np.average(self.times),
                                                                                            ttt))

            if self.session.tracker_on:
                self.myprint()

            self.last_timestamp_tmp =self.last_timestamp
            self.times = []
            # p = 100.0 * sample / nn
            # after = time.time()
            # tmp = after - base
            # tmp = (int(tmp / 60), int(tmp) % 60)
            #   '{2}/{3} = {0} % - {1:.1f} seconds. Total time spent: {4}:{5}'.format(p, (after - before), sample, nn,
            #                                                                          tmp[0], tmp[1]))
            # before = after
            # comparisons_all = 0

        self.session.logger.exit("NED_LSH_model.addDocument")

        return index

    def _addDocument(self, ID, itemText, metadata):
        self.session.logger.entry("NED_LSH_model._addDocument")

        if not self.tfidf_mode:
            itemText = self.process(itemText)

        if itemText.strip() == '':
            self.session.logger.exit("NED_LSH_model._addDocument")
            return -1

        self.text_data.append(itemText)
        self.id_list.append(ID)
        index = len(self.text_data) - 1
        self.text_metadata[ID] = metadata
        self.doc_indices[ID] = index

        if self.tfidf_mode:
            freq, doc = self.generateTFIDF(itemText)
        else:
            # determine if we have new words
            doc = self.word_vector(itemText)
            freq = doc

        doc_point = la.Document(ID, doc)
        self.repository[index] = doc_point
        #
        self.processed += 1

        #ID = self.id_list[sample]
        #ID = self.id_list[sample]
        #doc = self.counts[sample, :]

        #self.session.logger.debug('Adding document {0} ({2}) out of {1}'.format(sample, self.counts.shape[0], ID))
        nearest, nearestDist, comparisons, doc_point = self.lsh.add(doc_point)


        object = {
            '_id': doc_point.ID,
            'text' : self.text_metadata[doc_point.ID]['text'],
            'norm': doc_point.norm(),
            'thread': None,
            'leader': None,
            'vector': str(doc_point.v),
            'LSH-ID': None if nearest==None else nearest['point'].ID ,
            'LSH-TEXT': None if nearest==None else self.text_metadata[ nearest['point'].ID ]['text'],
            'LSH-norm': None if nearest==None else nearest['point'].norm(),
            'LSH-vector': None if nearest==None else str(nearest['point'].v),
            'LSH-distanse': nearestDist
        }


        #samer
        #if str(ID) == '255819967088451584':
        #    self.session.logger.info('samer: {0}'.format( json.dumps(object, indent=4, sort_keys=True) ) )

        data = self.text_metadata[ID]
        if self.first_timestamp == None:
            self.first_timestamp = data['timestamp']

        if self.last_timestamp == None or self.last_timestamp < data['timestamp']:
            self.last_timestamp = data['timestamp']

        if nearestDist == None or nearestDist > self.threshold:
            nearestDist1, nearest1 = self.searchInRecentDocs(doc_point)

            if nearestDist1 != None and (nearestDist == None or nearestDist1 < nearestDist):
                nearest = nearest1
                nearestDist = nearestDist1

                object['Recent-ID'] = None if nearest == None else nearest['point'].ID
                object['Recent-TEXT'] = None if nearest == None else self.text_metadata[nearest['point'].ID]['text']
                object['Recent-norm'] = None if nearest == None else nearest['point'].norm()
                object['Recent-distanse'] = nearestDist
                object['Recent-vector'] = None if nearest == None else str(nearest['point'].v)

        if self.session.output != None:
            self.session.output.classify_doc(doc_point.ID, object)

        nearestID = None
        if nearest != None:
            nearestID = nearest['point'].ID

        create_new_thread = False

        if nearestDist == None or nearestDist > self.threshold:
            create_new_thread = True

        nearThread = nearThreadID  = None
        if not create_new_thread:
            nearThreadID = self.tweet2thread.get(nearestID, None)
            if nearThreadID == None:
                wait = True #for easy debug purposes
            nearThread = self.threads_queue.get(nearThreadID, None)

            if nearThread == None or not nearThread.is_open(): #data['timestamp']):
                create_new_thread = True

                if nearThread != None:
                    #if nearThread.size() > 2:
                    #    nearThread.dump(self.text_metadata)

                    self.threads_queue.pop(nearThreadID)
                    self.tweet2thread.pop(nearestID)

        """
        nearThread = nearThreadID  = None
        if not create_new_thread:
            nearThreadID = self.tweet2thread[nearestID]
            nearThread = self.threads_queue.get(nearThreadID, None)

            while nearThread == None or not nearThread.is_open(data['timestamp']):
                print('enter loop', ID, nearThreadID, nearThread)
                if nearThread != None:
                    nearThread.dump(self.text_metadata)
                if self.threads_queue.get(nearThreadID, None) != None:
                    self.threads_queue.pop(nearThreadID)

                altr = self.alternatives.get(nearThreadID, None)
                if altr == None:
                    self.alternatives[nearThreadID] = ID
                    create_new_thread = True
                    break
                else:
                    nearThreadID = self.alternatives[nearThreadID]
                    nearThread = self.threads_queue.get(nearThreadID, None)

                    create_new_thread = False
        """


        if create_new_thread:
            self.threads[ID] = [ID]
            self.tweet2thread[ID] = ID
            self.threads_queue[ID] = TweetThread(self.session, ID, freq, data['user'], data['timestamp'], max_time_delta=self.max_thread_delta_time)

            msg = '*** NEW THREAD ***: new leader is {0} ("{1}"). '.format(ID, self.text_metadata[ID])


            if nearestDist != None:
                msg += '\n\t***Nearest thread leader is {0} with distance {2} (threshold {3}): ("{1}").'.format(nearestID,
                                                                                       self.text_metadata[nearestID]['text'],
                                                                                       nearestDist,
                                                                                       self.threshold)
            self.session.logger.debug(msg)



        else:
            nearThreadID = self.tweet2thread[nearestID]
            self.threads[nearThreadID].append(ID)
            self.threads_queue[nearThreadID].append(ID, freq, data['user'], data['timestamp'], nearestID, nearestDist)
            self.tweet2thread[ID] = nearThreadID
            self.session.logger.debug(
                '*** EXISTING THREAD ***: Add document {0} ("{1}") to existing thread {2} ("{3}").\n\t@@@Nearest document is {4} with distance {6}: ("{5}").'.format(
                    ID, self.text_metadata[ID]['text'], nearThreadID, self.text_metadata[nearThreadID]['text'],
                    nearestID, self.text_metadata[nearestID]['text'], nearestDist))

        self.session.logger.entry('NED_LSH_model.run.recent-docs')
        self.recent.append(ID)
        if len(self.recent) > self.recent_documents:
            self.recent = self.recent[1:]
        self.session.logger.exit('NED_LSH_model.run.recent-docs')

        self.session.logger.exit("NED_LSH_model._addDocument")
        return index

    def searchInRecentDocs(self, doc_point):
        self.session.logger.entry("searchInRecentDocs")
        nearestDist = None
        nearest = None
        # compare d to a fixed number of most recent documents
        flag = False
        for other in self.recent:
            # tmp = la.angular_distance(ID, other, doc, self.counts[self.doc_indices[other], :])
            other_doc = self.repository[self.doc_indices[other]]
            tmp = la.distance(doc_point, other_doc, logger=self.session.logger, auto_fix_dim=True)
            if nearestDist == None or nearestDist > tmp:
                nearestDist = tmp
                nearest = {'point' : other_doc}
                flag = True
        if flag:  # found a new neighbor
            self.session.logger.debug(
                '*** Search in Recent Documents: ***: {0} ("{1}") was found to be close to {2} ("{3}") distance {4}.'.format(
                    doc_point.ID, nearest['point'].ID, self.text_metadata[doc_point.ID]['text'], self.text_metadata.get(other, ''), tmp))

        self.session.logger.exit("searchInRecentDocs")
        return nearestDist, nearest

    def myprint(self):
        self.session.logger.debug('*******************************************')
        self.lsh.myprint()


    def dumpThreads(self, filename, max_threads):
        #self.session.logger.entry('dumpThreads')
        file = codecs.open(filename, 'w', encoding='utf-8')
        
        ttt = human_time( seconds=self.last_timestamp - self.first_timestamp )
        file.write('Printing {1} threads... total period: {0}\n'.format( ttt, min(max_threads, len(self.threads_queue) ))) 
        thr = 1
        for x in sorted(self.threads, key=lambda x: len(self.threads[x]), reverse=True):
            threadSize = len(self.threads[x])
            
            #if threadSize<3:    
            #    #not interesting anymore
            #    break
            self.session.logger.debug('Thread: {0}, size: {1} documents'.format(x, threadSize))
            text = self.text_metadata[x]['text'] #.replace('\t', ' ')
            #text = text.encode(encoding='utf-8')
            file.write('\n' + '-'*40 + ' THREAD {0} - {1} documents score: {2} and {3} users'.format(thr, threadSize, 0, 0) + '-'*40 + '\n')
            file.write('Leader is {0}: "{1}"\n'.format(x, text))
            file.write('thread\tleading doc\titem#\titem ID\tuser\titem text\titem text(original)\n')
            c = 1
            for item in self.threads[x]:
                i = self.doc_indices[item]
                text1 = self.text_data[i]
                text2 = self.text_metadata[item]['text'] 
                user = self.text_metadata[item]['user']
            
                file.write('{0}\t{1}\t{2}\t{3}\t{4}\t"{5}"\t"{6}"\n'.format( thr, x, c, item, user, text1, text2 ))
                c+=1
            thr += 1
            if thr>max_threads:
                break
            
        file.close()
        #self.session.logger.exit('dumpThreads')
       
    def dumpThreads2(self, filename, max_threads):
        #self.session.logger.entry('dumpThreads')
        file = codecs.open(filename, 'w', encoding='utf-8')

        ttt = human_time(seconds=self.last_timestamp - self.first_timestamp)
        file.write('Printing {1} threads... total period: {0}\n'.format( ttt, min(max_threads, len(self.threads_queue) )))
        thr = 1
        for x in sorted(self.threads_queue, key=lambda x: self.threads_queue[x].size(), reverse=True):
            threadSize = self.threads_queue[x].size()
            
            #if threadSize<3:    
            #    #not interesting anymore
            #    break
            
            self.session.logger.debug('Thread: {0}, size: {1} documents'.format(x, threadSize))
            text = self.text_metadata[x]['text'] #.replace('\t', ' ')
            #text = text.encode(encoding='utf-8')
            ttt = human_time(seconds=self.threads_queue[x].thread_time())
            file.write('\n' + '-'*40 + ' THREAD {0} - {1} documents entropy: {2} and {3} users. period of {4} seconds'.format(thr, threadSize, self.threads_queue[x].entropy(), self.threads_queue[x].users_count(), ttt) + '-'*40 + '\n')
            file.write('Leader is {0}: "{1}"\n'.format(x, text))
            file.write('thread\tleading doc\titem#\titem ID\tuser\tnearest ID\tdistance\titem text\titem text(original)\n')
            c = 1
            for item in self.threads_queue[x].idlist:
                i = self.doc_indices[item]
                text1 = self.text_data[i]
                text2 = self.text_metadata[item]['text'] 
                user = self.text_metadata[item]['user']
                nearID = self.threads_queue[x].document_contents[item][0]
                nearestDist = self.threads_queue[x].document_contents[item][1]
                file.write('{0}\t{1}\t{2}\t{3}\t{7}\t{8}\t{4}\t"{5}"\t"{6}"\n'.format( thr, x, c, item, user, text1, text2, nearID, nearestDist ))
                c+=1
            thr += 1
            if thr>max_threads:
                break
            
        file.close()
        #self.session.logger.exit('dumpThreads')
      
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
        
    def dumpThreads3(self, filename, max_threads):
        #self.session.logger.entry('dumpThreads')
        file = codecs.open(filename, 'w', encoding='utf-8')

        ttt = human_time(seconds=self.last_timestamp - self.first_timestamp)
        file.write('Printing {1} threads... total period: {0}\n'.format( ttt, min(max_threads, len(self.threads_queue) )))
        thr = 1
        for x in sorted(self.threads_queue, key=lambda x: self.helper_lambda(x), reverse=True):
            threadSize = self.threads_queue[x].size()
            
            #if threadSize<3:    
            #    #not interesting anymore
            #    break
            
            self.session.logger.debug('Thread: {0}, size: {1} documents'.format(x, threadSize))
            text = self.text_metadata[x]['text'] #.replace('\t', ' ')
            #text = text.encode(encoding='utf-8')
            ttt = human_time(seconds=self.threads_queue[x].thread_time())
            isOpen = ''
            if not self.threads_queue[x].is_open():
             isOpen = ' [CLOSED]'

            file.write('\n' + '-'*40 + ' THREAD {0}{5} - {1} documents score: {2} and {3} users. period of {4}'.format(thr, threadSize, self.threads_queue[x].entropy(),
                                                                                                                          self.threads_queue[x].users_count(), ttt,
                                                                                                                          isOpen) + '-'*40 + '\n')
            file.write('Leader is {0}: "{1}"\n'.format(x, text))
            file.write('thread\tleading doc\titem#\titem ID\tuser\ttimestamp\tnearest ID\tdistance\titem text\titem text(original)\n')
            c = 1
            for item in self.threads_queue[x].idlist:
                i = self.doc_indices[item]
                text1 = self.text_data[i]
                text2 = self.text_metadata[item]['text'] 
                user = self.text_metadata[item]['user']
                timestamp = self.text_metadata[item]['created_at']
                nearID = self.threads_queue[x].document_contents[item][0]
                nearestDist = self.threads_queue[x].document_contents[item][1]
                file.write('{0}\t{1}\t{2}\t{3}\t{4}\t{7}\t{8}\t"{5}"\t"{6}"\t"{9}"\n'.format( thr, x, c, item, user, timestamp,
                                                                                       text1, text2, nearID, nearestDist ))
                c+=1
            if self.threads_queue[x].is_open():
                thr += 1

            if thr>max_threads:
                break
            
        file.close()
        #self.session.logger.exit('dumpThreads')
       
