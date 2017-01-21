# -*- coding: utf-8 -*-
"""
Created on Sun Oct 23 14:01:57 2016

@author: SAMERA
"""
from datetime import datetime
import sys
import threading
import matplotlib.pyplot as plt

import matplotlib.pyplot as plt
import numpy as np

def scatter_plot_with_correlation_line(x, y, graph_filepath, title):
    '''
    http://stackoverflow.com/a/34571821/395857
    x does not have to be ordered.
    '''

    plt.figure()
    # Scatter plot
    plt.scatter(x, y)
    plt.title(title)
    # Add correlation line
    axes = plt.gca()
    m, b = np.polyfit(x, y, 1)
    X_plot = np.linspace(axes.get_xlim()[0],axes.get_xlim()[1],100)
    plt.plot(X_plot, m*X_plot + b, '-')

    # Save figure
    plt.savefig(graph_filepath, dpi=300, format='png', bbox_inches='tight')


    plt.close()


def scatter_plot_with_correlation_line1(x, y, graph_filepath, title):
    m, b = np.polyfit(x, y, 1)

    # plt.scatter(range(0, len(self.execution_times[func])), self.execution_times[func])
    plt.figure()

    plt.ylabel('execution times (s)')
    plt.title(title)

    plt.plot(x, y, '.')
    y1 = (m * x) + b
    plt.plot(x, y1, '-')

    plt.ylim([min(y)*1, max(y)*1.5])
    plt.xlim([-0.5, max(x)+1])

    if graph_filepath == None:
        plt.show()
    else:
        plt.savefig(graph_filepath)

    plt.close()

def generate_key(func_name):
    th = threading.current_thread()
    key = ' - '.join( [str(th) , func_name] )
    return key

class simplelogger:
    FINE = 5
    DEBUG = 4
    INFO = 3
    WARN = 2
    ERROR = 1
    CRITICAL = 0

    handlers = None
    loglevels= None
    profiling = False
    profiling_res = {}
    execution_times = {}
    helper = {}

    def init(self, filename=None, std_level=INFO, file_level=DEBUG, profiling=False, bufsize = 10):
        self.loglevels = [std_level, file_level]

        self.profiling_res = {}
        self.execution_times = {}
        self.helper = {}
        self.turn_profiling( profiling )
        
        self.handlers = [sys.stdout]
        if filename != None:
            file = open(filename, 'w')
            self.handlers.append (file)


    def checklevel(self, level, handler):
        return self.loglevels[handler] >= level

    def info(self, message):
        for i in range(len(self.loglevels)):
            if self.checklevel(self.INFO, i):
                self.write(i, 'INFO', message)

    def fine(self, message):
        for i in range(len(self.loglevels)):
            if self.checklevel(self.FINE, i):
                self.write(i, 'FINE', message)

    def debug(self, message):
        for i in range(len(self.loglevels)):
            if self.checklevel(self.DEBUG, i):
                self.write(i, 'DEBUG', message)

    def warning(self, message):
        for i in range(len(self.loglevels)):
            if self.checklevel(self.WARN, i):
                self.write(i, 'WARNING', message)
                    
    def error(self, message):
        for i in range(len(self.loglevels)):
            if self.checklevel(self.ERROR, i):
                self.write(i, 'ERROR', message)
                
    def critical(self, message):
        for i in range(len(self.loglevels)):
            if self.checklevel(self.CRITICAL, i):
                self.write(i, 'CRITICAL', message)
        
    def turn_profiling(self, on=True):
        if on and not self.profiling:
            self.profiling_res = {}
            self.execution_times = {}
            self.helper = {}
        self.profiling = on

    def entry(self, func):
        if not self.profiling:
            return 

        key = generate_key(func)
            
        ts = datetime.now()
        
        self.helper[key] = ts


    def exit(self, func):
        if not self.profiling:
            return 
            
        key = generate_key(func)
        entry_time = self.helper.get(key, None)
        
        assert(entry_time != None)
        self.helper[key] = None

        ts = datetime.now()

        times = self.execution_times.get(key, None)
        if times==None:
            times = list()
            self.execution_times[key] = times
        d = ts - entry_time
        d = d.total_seconds()
        times.append(d)

        temp, count = self.profiling_res.get(key, (0, 0))
        self.profiling_res[key] = (temp + d, count+1)

        #self.debug('Exit: {0} at {1}'.format(func, ts))

    def profiling_dump(self, path=None, avg_base=None):
        for func in sorted( self.profiling_res , key=self.profiling_res.get, reverse=True):
            seconds, count = self.profiling_res[func]

            #if seconds < 0.01:
            #    continue

            mins = int(seconds / 60.0)
            secs = seconds - mins * 60

            msg = 'invoked {1:10} times, total {2}\' {3:.6f}\'\' - {0}'.format(func, count, mins, secs)
            if avg_base!=None:
                tmp = float(avg_base)
                mins = mins / tmp
                secs = secs / tmp
                msg += '\t- average {0:.6f} s ({1:.6f} min) based on {2} iterations'.format(secs, mins, avg_base)

            self.info(msg)

        i = 0
        for func in sorted( self.profiling_res , key=self.profiling_res.get, reverse=True):
            self.info('Function {0} ran {1} times'.format(func, len(self.execution_times[func])))
            #print(self.execution_times[func])

            if self.profiling_res[func][1] < 3:
                continue

            x = range(0, len(self.execution_times[func]))
            y = self.execution_times[func]

            scatter_plot_with_correlation_line(x, y, path + '/' + str(i) + '.png', func)
            #scatter_plot_with_correlation_line1(x, y, path + '/' + str(i) + '.png', func)

            i += 1


    def write(self, handler, levelname, message):
        dt = datetime.now()
        text = '{:{dfmt} {tfmt}}'.format(dt, dfmt='%Y-%m-%d', tfmt='%H:%M')
        text += ' ' + levelname + ': ' + message + '\n'
        

        #if 'c:/temp/0000010_docs_round_00.log' == self.handlers[1].name:
        #    debug = 1

        try:
            self.handlers[handler].write(text)
        except UnicodeEncodeError as e:
            tmp = text.encode('ascii', 'ignore')
            tmp = tmp.decode('utf-8')
            self.handlers[handler].write(tmp)
        except Exception as unexpected:
            #print('Received error ({0}) while writing to log file ({2})'.format(unexpected, text, self.handlers[handler]))
            raise
        
    def flush(self):
        if self.handlers[1] != None:
            self.handlers[1].flush() #.close()
            
    def close(self):
        if self.handlers[1] != None:
            self.handlers[1].close()
            #print('Log file ({0}) is closed now'.format(self.handlers[1]))
            

