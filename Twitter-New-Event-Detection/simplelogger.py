# -*- coding: utf-8 -*-
"""
Created on Sun Oct 23 14:01:57 2016

@author: SAMERA
"""


from datetime import datetime
import sys
import threading
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
    #axes = plt.gca()
    m, b = np.polyfit(x, y, 1)
    #X_plot = np.linspace( axes.get_xlim()[0],axes.get_xlim()[1],100)
    X_plot = np.linspace(min(x), max(x), 100)
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

        if entry_time == None:
            #ignore
            return

        #assert(entry_time != None)
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

    def profiling_dump(self, human_time, path=None, avg_base=None):
        for func in sorted( self.profiling_res , key=self.profiling_res.get, reverse=True):
            seconds, count = self.profiling_res[func]

            #if seconds < 0.01:
            #    continue

            mins = int(seconds / 60.0)
            secs = seconds - mins * 60

            msg = 'invoked {1:10} times, total {2} - {0}'.format(func, count, human_time(seconds=secs))
            if avg_base!=None:
                tmp = float(avg_base)
                mins = mins / tmp
                secs = secs / tmp
                msg += '\t- average {0} based on {1} iterations'.format(human_time(seconds=secs), avg_base)

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
            

if __name__ == '__main__':

    x = range(0, 459)
    y = [1.008058, 0.597034, 0.968055, 1.347077, 0.791045, 0.909052, 0.677038, 0.970055, 0.859049, 0.640037, 0.729042, 0.0, 0.936054, 1.150066, 0.87205, 1.460083, 1.003057, 1.155066, 0.630036, 0.0, 0.760043, 0.709041, 1.077061, 1.411081, 0.959055, 1.295074, 0.001, 1.306075, 0.762043, 0.998057, 0.725041, 0.909052, 0.686039, 1.252072, 1.138065, 0.0, 0.0, 0.827047, 1.659094, 1.23407, 0.606035, 1.131065, 1.366078, 0.500029, 1.178067, 0.0, 0.0, 0.630036, 1.05206, 0.721041, 0.0, 1.577091, 0.962055, 1.353078, 0.0, 0.0, 1.819104, 0.617035, 1.526087, 1.518087, 0.607034, 1.21807, 0.0, 1.738099, 1.219069, 0.0, 1.21307, 1.268073, 0.001, 1.735099, 2.581148, 2.566147, 1.126064, 2.577147, 1.673095, 2.678154, 1.436082, 3.056175, 1.7591, 0.0, 1.04606, 2.030116, 0.766044, 1.683096, 1.93211, 1.418081, 0.725042, 2.232128, 0.0, 1.259072, 1.394079, 2.215126, 1.494085, 0.0, 0.87805, 0.0, 1.841105, 0.001, 0.0, 0.0, 3.632208, 3.170181, 1.513087, 1.026059, 2.216126, 0.901052, 3.031174, 0.0, 1.494086, 0.0, 1.934111, 0.001, 1.801103, 2.053118, 0.731042, 2.998172, 1.429082, 0.0, 1.298074, 1.544088, 1.503086, 0.0, 1.024059, 0.0, 2.308132, 0.951055, 0.766044, 0.0, 2.155123, 2.26913, 2.565146, 0.0, 2.365135, 1.543088, 2.066119, 1.728099, 2.679153, 3.14318, 2.054117, 2.124122, 0.849048, 0.892051, 1.734099, 1.865106, 1.7381, 1.411081, 2.063118, 0.001001, 3.634207, 3.827218, 0.001, 1.836105, 2.072118, 0.0, 2.09512, 0.0, 3.239185, 2.009115, 1.502086, 1.684096, 0.0, 0.001, 2.825161, 1.423081, 2.336134, 2.680153, 1.7381, 0.0, 2.195125, 1.683096, 2.385136, 0.0, 0.0, 3.311189, 0.0, 3.549203, 0.001, 2.481142, 0.0, 2.664152, 2.019115, 0.930054, 1.444082, 2.056118, 2.670153, 3.224184, 2.186125, 2.44214, 3.375193, 2.177124, 2.396137, 0.0, 2.046117, 1.852106, 1.40208, 0.0, 2.650152, 0.0, 2.725156, 3.696211, 1.722098, 1.474084, 0.001, 0.0, 1.549088, 0.001, 2.126122, 2.043117, 3.218184, 2.874164, 1.977113, 2.950169, 0.0, 2.206126, 2.076119, 0.0, 0.0, 2.787159, 1.58009, 0.0, 0.0, 2.036116, 0.001, 0.0, 0.975056, 2.785159, 3.167181, 0.0, 3.624207, 2.258129, 3.390194, 4.085233, 1.853106, 1.927111, 1.708097, 2.023116, 0.0, 0.0, 1.873107, 2.526144, 1.913109, 3.774216, 3.813219, 0.0, 0.0, 3.4892, 3.639208, 2.536145, 2.310132, 0.0, 0.001, 1.966112, 2.055118, 3.126179, 2.80616, 2.735157, 4.138237, 0.0, 3.32619, 2.819161, 2.571147, 3.314189, 3.308189, 3.741214, 2.216127, 3.536203, 3.007172, 0.0, 2.604149, 3.088177, 2.594148, 2.769159, 3.33319, 3.403195, 2.839162, 2.219127, 0.001, 4.512258, 0.001, 5.258301, 0.001, 4.084234, 2.26713, 0.0, 1.938111, 3.182182, 2.97317, 1.481085, 0.001, 0.0, 2.549146, 0.001, 2.170124, 0.0, 2.192126, 3.895223, 2.770159, 2.667152, 1.522087, 2.731157, 3.752215, 3.231184, 4.096234, 0.0, 0.0, 0.001, 0.001, 3.268187, 4.37325, 4.626265, 2.647151, 2.61515, 3.429196, 2.465141, 3.423196, 3.193183, 3.053174, 2.44414, 0.0, 2.044116, 2.732157, 3.590206, 4.01423, 3.951226, 0.0, 3.731214, 2.507143, 3.31519, 0.0, 0.001, 0.0, 3.401195, 0.0, 2.79616, 2.793159, 3.771216, 2.996171, 2.823161, 3.815218, 4.597263, 3.652209, 4.070233, 0.0, 2.566146, 4.324248, 2.817161, 0.001, 1.855106, 0.001, 4.477256, 3.766215, 4.385251, 3.330191, 0.0, 0.0, 0.0, 4.039231, 5.829333, 1.915109, 0.001, 0.0, 4.969285, 0.0, 4.54726, 3.210184, 0.0, 4.406252, 1.844106, 4.02323, 4.937282, 3.605206, 3.407195, 2.840162, 5.697326, 4.736271, 2.446139, 3.13618, 6.368364, 0.0, 4.36925, 0.0, 2.889166, 0.0, 0.0, 2.941169, 0.001, 2.893165, 4.476256, 3.500201, 4.622264, 0.0, 2.444139, 5.584319, 3.926225, 4.400252, 0.0, 5.006287, 2.501143, 3.32319, 0.0, 4.807275, 4.489257, 0.0, 0.0, 3.360192, 2.517144, 3.611206, 4.715269, 3.630208, 3.158181, 5.578319, 0.0, 3.203183, 0.0, 5.708327, 6.428368, 6.000343, 0.0, 4.036231, 4.654266, 2.940168, 2.829162, 3.288188, 4.107235, 4.19224, 3.086176, 5.097292, 5.127293, 5.115293, 0.0, 0.0, 3.4922, 3.4942, 4.317247, 3.466198, 6.383365, 4.995285, 0.0, 0.0, 4.397251, 3.15318, 3.541202, 5.873336, 6.581376, 4.624265, 4.440254, 3.792217, 2.430139, 2.043117, 4.397251, 6.701384, 0.0, 3.649208, 4.88728, 4.866279, 3.826218, 0.0, 3.261187, 5.831334]

    scatter_plot_with_correlation_line(x, y, 'c:\\temp\\aaa.png', 'Test')
