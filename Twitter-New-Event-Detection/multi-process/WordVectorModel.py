
from scipy.sparse import csr_matrix , lil_matrix
import numpy as np
import time
import pprint
from simple_twitter_parser import clean_spaces


class Corpus:
    _word2index = {}
    _index2word = []

    def __init__(self):
        self._word2index = {}
        self._index2word = []

    def word2index(self, word):
        return self._word2index[word]

    def index2word(self, index):
        return self._index2word[index]

    def getIndex(self, word):
        idx = self._word2index.get(word, -1)
        return idx

    def add(self, word):

        idx = self._word2index.get(word, -1)
        if idx == -1:
            idx = len(self._index2word)
            self._word2index[word] = idx
            self._index2word.append( word )

        return idx

    def print(self):
        pprint.pprint ( self._word2index )

    def size(self):
        return len(self._index2word)

class WordVectorModel:
    corpus = Corpus()
    id_list = []
    indices = {}
    metadata = {}

    def __init__(self):
        self.corpus = Corpus()

        self.id_list = []
        self.indices = {}
        self.metadata = {}


    def word_vector(self, text, max=False):
        #self.count_vect = CountVectorizer()  # stop_words='english')
        #self.counts = self.count_vect.fit_transform(text_data)


        words = clean_spaces( text ) #.split()
        tmpsize = self.corpus.size() + len(words)
        word_counts = lil_matrix((1, tmpsize), dtype=np.int8)

        for w in words:
            idx = self.corpus.add(w)
            word_counts[0, idx] += 1

        retVal = word_counts
        if max:
            maximum = 0
            for w in words:
                idx = self.corpus.getIndex(w)
                if maximum < word_counts[0, idx]:
                    maximum = word_counts[0, idx]
            retVal = word_counts, maximum

        return retVal


class TFIDFModel:
    wv = WordVectorModel()
    word_appearances = {}
    document_count = 0

    weight = None

    def __init__(self, initial_dim):
        self.wv = WordVectorModel()
        self.word_appearances = {}
        self.idf = []
        self.document_count = 0

        self.weight = lil_matrix((1, initial_dim), dtype=np.float64)

    def addDoc(self, d):
        # split to words, calculate f(w, d)
        #    add each w to the corpus
        #    calculate sparse vector of word count
        #d = d.lower()
        f = self.wv.word_vector(d)

        self.document_count += 1

        weight = lil_matrix(f.shape, dtype=np.float64)
        for k in f.nonzero()[1] :
            self.word_appearances[k] = self.word_appearances.get(k, 0) + 1
            p1 = np.log2(self.document_count + 0.5) / self.word_appearances[k]
            p2 = np.log2(self.document_count + 1.0)

            weight [0, k] = p1 / p2 * f[0, k]

        return f, weight.tocsr()


"""
    def addDoc1(self, text):
        word_counts, m = self.wv.word_vector(text, max=True)

        data = []
        col = []
        row = []

        tf = word_counts * (0.5 / m) + 0.5

        self.document_count += 1

        tfidf = lil_matrix(word_counts.shape, dtype=np.float64)
        for k in word_counts.nonzero() :
            self.word_appearances[k] = self.word_appearances.get(k, 0) + 1
            self.idf[k] = self.document_count / self.word_appearances[k]
            tfidf[0, k] = tf[0, k] * self.idf[k]

        #mat = csr_matrix((data, (row, col)), shape=(1, self.wordindex_next))

        return tfidf
"""

if __name__ == '__main__':
    init_dim = 1000
    tfidf = TFIDFModel(initial_dim=init_dim)

    text = [
        'this is a test',
        'function has parameters',
        'amdocs is company ltd.',
        'this is a blue sky',
        'this is a blue sky',
        'this is a blue sky',
        'this is a blue sky company'
    ]

    base = time.time()
    for i in range(1):
        for t in text:
            res2 = tfidf.addDoc(t)
            print (t)
            print (res2)

    print(time.time() - base)
    print ( tfidf.wv.corpus.print() )



