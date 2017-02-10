

from pymongo import MongoClient
import pymongo
from sklearn.feature_extraction.text import CountVectorizer
from datetime import datetime
import sys
import threading
import matplotlib.pyplot as plt
from scipy.sparse import hstack
import math
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import pairwise_distances
#from scipy.spatial import distance as dist
#import linalg_helper as lh
from scipy import sparse
from scipy import stats
import threading
import time
from subprocess import call
from subprocess import Popen
from scipy.sparse import csr_matrix , lil_matrix
import numpy as np
import time
import pprint

from nltk.corpus import stopwords
import nltk

try :
    eng_stopwords = set(stopwords.words('english'))
except LookupError as le:
    nltk.download()