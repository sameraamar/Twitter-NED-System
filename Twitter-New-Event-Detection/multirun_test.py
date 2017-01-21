# -*- coding: utf-8 -*-
"""
Created on Wed Oct 26 09:54:43 2016

@author: SAMER AAMAR
"""


import time
#from subprocess import call
from subprocess import Popen
#from multirun import run_all
 
#%%

from pymongo import MongoClient


dbname = 'test'
collname = 'results'
host = 'localhost'
port = '27017'

url = 'mongodb://samer:samer@ds052629.mlab.com:52629/tweets1'
client = MongoClient(url) #host, int(port))
dbname = 'tweets1'
db = client[dbname]
dbcoll = db[collname]

      

def func1(src, data, key):
    #print ("data={0}; key={1}".format(str(data), str(key)))
    c = 0
    for i in range(key):
        c += data
    print('func1', src)
    dbcoll.insert_one({ 
                      'func' : 'func1', 
                      'src' : src, 
                      'data' : data,
                      'key': key, 
                      'result' : c, 
                     })
    return c
    
def func2(src, data, key):
    #print ("data={0}; key={1}".format(str(data), str(key)))
    c = 1
    m = 1
    for i in range(50000):
        m *= c
        c += 1
    
    print('func2', src)
    dbcoll.insert_one({ 
                      'func' : 'func2', 
                      'src' : src, 
                      'data' : data,
                      'key': key, 
                      'result' : str(m), 
                     })

    return c,m

#%%
if __name__ == '__main__':
    
    
    iterations = 20
    
    print('\nregular call:\n')
    base = time.time()
    for i in range(iterations):
        func1('regular', 340000, 3)
        func2('regular', 3, 2)
    
    print('spent: ' + str(time.time() - base) + ' seconds')
    
#%%
#    print('\nnow let\'s try with threads:\n')
#    
#    base = time.time()
#    for i in range(iterations):
#        run_all([ (func1, 'mthread', 340000, 3) , [func2, 'mthread', 3, 2] ] )
#       
#    print('spent: ' + str(time.time() - base) + ' seconds')
    
#%%    

    
    
    cmd1 = 'python -c "import  multirun_test; multirun_test.func1 (\'shell\', 340000, 3)"' 
    cmd2 = 'python -c "import  multirun_test; multirun_test.func2 (\'shell\', 3, 2)"'
    base = time.time()
    
    print('now trying child processes')
    
    subprocesses = []
    
    for i in range(iterations):
        subprocesses.append(Popen(cmd1 , shell=True))
        #print( cmd1 )
        subprocesses.append( Popen(cmd2 , shell=True ) )
        #print( cmd2) 
    
        
    print('spent: ' + str(time.time() - base) + ' seconds')
    for p in subprocesses:
        p.wait()
        
    print('spent: ' + str(time.time() - base) + ' seconds')


