from sklearn.feature_extraction.text import CountVectorizer

corpus = [
    'This is the first document.',
    'This is the second second document.',
    'And the third one.',
    'Is this the first document?',
]

#%%
# Method 1:

from sklearn.feature_extraction.text import TfidfTransformer
vectorizer = CountVectorizer()  # stop_words='english')
counts = vectorizer.fit_transform(corpus)

transformer = TfidfTransformer(smooth_idf=False)

tfidf = transformer.fit_transform(counts)
tfidf.toarray()

print(tfidf)
print(transformer.idf_)
print('done.')

#%%
# Method 2:

from sklearn.feature_extraction.text import TfidfVectorizer
vectorizer = TfidfVectorizer(min_df=1)
tfidf = vectorizer.fit_transform(corpus)

print(tfidf)
print(vectorizer.idf_)
print(vectorizer.vocabulary_)
print('done.')

#%%
# Method 3:

from WordVectorModel import TFIDFModel
import time

init_dim = 10
tfidf = TFIDFModel(initial_dim=init_dim)

#base = time.time()
for i in range(1):
    for t in corpus:
        res2 = tfidf.addDoc(t)
        print(t)
        print(res2)

#print(time.time() - base)
print( tfidf.wv.corpus.print())
print ( tfidf.idf_ )