# -*- coding: utf-8 -*-
"""
Created on Sat Nov  5 21:50:08 2016

@author: unknown

I have copied this peice of python code from the internet and the credit goes to https://marcobonzanini.com
Anyway, I improved it by making it more reusable module with much more parameters

"""

#%%
import re, string
from nltk.corpus import stopwords


emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""

url_str = r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+'
hashtag_str = r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"
mention_str = r'(?:@[\w_]+)'
number_str = r'(?:(?:\d+,?)+(?:\.?\d+)?)'

regex_str = [
    emoticons_str,
    r'<[^>]+>', # HTML tags
    mention_str, # @-mentions
    hashtag_str, # hash-tags
    url_str, # URLs
    number_str, # numbers
    #r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    #r"(?:[a-z][a-z\-_]+[a-z])", # words with - and _
    r"(?:[a-z][a-z']+[a-z])", # words with '
    r"(?:&[A-Za-z]+;)", # &gt; &lt; etc.
    r'(?:[\w_]+)', # other words
    r'(?:\S)' # anything else
]
    
tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)
mention_re = re.compile(mention_str+'$', re.VERBOSE | re.IGNORECASE)
number_re = re.compile(number_str+'$', re.VERBOSE | re.IGNORECASE)
url_re = re.compile(url_str+'$', re.VERBOSE | re.IGNORECASE)
hashtag_re = re.compile(hashtag_str+'$', re.VERBOSE | re.IGNORECASE)
html_re = re.compile(r"(?:&[A-Za-z]+;)", re.VERBOSE | re.IGNORECASE)

puncchars = list(string.punctuation)
eng_stopwords = set(stopwords.words('english'))
ily_stopwords = set(stopwords.words('italian'))

# -*- coding: utf-8 -*-
def isEnglish(s):
    try:
        s.encode('ascii')
    except Exception:
        return False
    else:
        return True

def tokenize(s):
    return tokens_re.findall(s)

def clean_spaces(s):
    return preprocess(s, emoicons=False, numbers=True, mentions=False, url=True, hashtag=True, punctuation=False, return_text=False, stop_words=True, ignore_retweet=True, english_only=True)

def preprocess(s, lowercase=True, emoicons=True, numbers=True, mentions=True, url=True, hashtag=True, punctuation=True, return_text=False, stop_words=True, ignore_retweet=False, english_only=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]


    tmp = []
    for token in tokens:

        if english_only and not isEnglish(token):
            continue

        if not emoicons and emoticon_re.search(token):
            continue
        if not punctuation and token in puncchars:
            continue
        if not stop_words and (token in ily_stopwords or token in eng_stopwords):
            continue
        if not url and url_re.search(token):
            continue
        if not hashtag and hashtag_re.search(token):
            continue
        if not mentions and mention_re.search(token):
            continue
        if not numbers and number_re.search(token):
            continue
        if html_re.search(token):
            continue
        if token == 'rt' or token == 'RT' and len(tmp)==0:
            if ignore_retweet:
                token = 'rt'
                break

            continue

        
        tmp.append(token)

    if token == 'rt':
        tmp = []

    tokens = tmp
    
    if return_text:
        res = ' '.join(tokens)
        return res.strip()
    
    return tokens


if __name__ == '__main__':
    tweets = ["""RT @marcobonzanini: Just an  6.5 5.8889 it's you're   #this-is-a-combination  Example!
    :D 
    http://example.com #NLP""" ,
            "Ayyy you from my hood what school you went to?  &gt; Wanna show you how much I'm dedicated to you.   ",
            'RT @kumailn: Wanna go see the Kevin James movie just so at the start' \
            ' I can yell out super loud "Here comes the here comes the BOOM!" The ...' ,
              "RT @Sherm_YG: \ue32e#90sBabyFollowTrain 1.\ue232\ue233\ue235\ue234\ue236\ue237\ue238\ue239 Retweet this\ue420 2. I'll \ue536\ue201You 3. You Will\ue201\ue115\ue330 Back 4. Or Get Unfollowed\ue00d\ue333\ue023 5. FAV for a S/O \ue00e ..."]


    for tweet in tweets:
        print('shape 1')
        print(preprocess(tweet, lowercase=False, numbers=True, return_text=True, emoicons=True, mentions=True, url=True, hashtag=True, punctuation=True, stop_words=True))
        print('shape 1')
        print(preprocess(tweet, return_text=True, mentions=False, stop_words=False, hashtag=True))

    for tweet in tweets:
        print(clean_spaces(tweet))




            #text = text.replace('\t', ' ').replace('\n', ' ')
        ##text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(&[A-Za-z]+;)|(#[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",text).split())
        #text = ' '.join(re.sub("(@[A-Za-z0-9]+)|(&[A-Za-z]+;)|([^0-9A-Za-z# \t])|(\w+:\/\/\S+)"," ",text).split())
        #return text.lower().strip()    