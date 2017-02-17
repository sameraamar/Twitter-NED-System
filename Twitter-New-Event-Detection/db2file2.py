import pymongo
from pymongo import MongoClient
import time
from session import Session
import json, gzip

port = 27017
#host = '192.168.1.102'
host = 'localhost'
#dbname = 'events2012'
dbname = 'petrovic' #'events2012'
collname = 'topics'
path2data = ''
prefix = 'topics'
#dbcoll = 'relevance_judgments'

offset = 0
lines_per_file = 500000
client = MongoClient(host, port)
db = client[dbname]
dbcoll = db[collname]


session = Session(tracker_on=False)
#log_filename = path.join(folder , 'audit.log')
#print(log_filename)

tmp = session.generate_temp_folder('DB2TXT2')

count = 0

print ('Output folder: ', tmp)
file = None
done = False
while not done:
    cursor = dbcoll.find({})
    cursor.sort("_id", pymongo.ASCENDING)
    cursor.skip(offset)
    print('Query on mongodb performed (skipped {0} documents)'.format(offset))
    printme = True
    try:
        for item in cursor:

            if count % lines_per_file == 0:
                if file != None:
                    file.close()

                fullfilename = '{0}/{1}_{2:012d}.txt.gz'.format(tmp, prefix, count + offset)
                file = gzip.open(fullfilename, 'wt', encoding='utf-8')
                #file = gzip.open(tmp + '/, 'wt', encoding='utf-8')

            if (printme):
                print('New item is: {0}.'.format(item))
                printme = False

            data = item
            if item.get(path2data, None) != None:
                data = item[path2data]

            file.write(json.dumps(data, sort_keys = True, ensure_ascii=False))
            file.write('\n')

            if count % 500 == 0:
                print('Cursor index {0}. Processed successfully {1}'.format(count+offset, count))

            count += 1
            lasttime = time.time()
        done = True
    except Exception as e:
        print (e)
        raise

file.close()