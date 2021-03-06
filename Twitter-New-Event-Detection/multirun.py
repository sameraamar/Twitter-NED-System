# -*- coding: utf-8 -*-
"""
Created on Wed Oct 26 09:54:43 2016

@author: SAMER AAMAR
"""


import threading
import time
from subprocess import call
from subprocess import Popen

a = None

class FuncThread(threading.Thread):
    results = None
    
    def __init__(self, target, *args):
        threading.Thread.__init__(self)
        self._target = target
        self._args = args 
        
        
    def run(self):
        assert( self._target!= None )
        if self._args != None and len(self._args)>0:
            self.results = self._target(*self._args[0])
        else:
            self.results = self._target()
        
#    def run_all_debug(func):
#        results = []
#        for i in range(len(func)):
#            f = func[i]
#            param = f[1:]
#            f = f[0]
#            res = f(*param)
#            results.append(res)
#            
#        return results

def run_bg(func):
    threads = []

    for i in range(len(func)):
        f = func[i]
        param = None
        if len(f) > 1:
            param = f[1:]
        f = f[0]
        if param != None:
            t = FuncThread(f, param)
        else:
            t = FuncThread(f)

        t.start()
        threads.append(t)
    return threads
    
def run_all(func, debugging=False):
#        if debugging:
#            return run_all_debug(func)
        
    threads = []

    for i in range(len(func)):
        f = func[i]
        param = f[1:]
        f = f[0]
        if param != None:
            t = FuncThread(f, param)
        else:
            t = FuncThread(f)

        t.start()
        threads.append(t)
        
    # Wait for all threads to complete
    #print ('********** waiting for threads to complete')
    results = []
    for t in threads:
        t.join()
    for t in threads:
        results.append(t.results)
        #print ('Finished: ' + str(t.results))
    #print ('********** threads are done')
    return results
 
    