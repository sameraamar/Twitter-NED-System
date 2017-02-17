from action_listener import TextGZipStreameSet, Listener
from session import Session
from simplelogger import simplelogger
from pprint import pprint
from os import path
from SNA_listener import SNAListener


if __name__ == '__main__':
    offset = 0
    inputfiles_path = 'C:\\data\\events_db\\petrovic'
    filenames = ['petrovic_00000000.gz', 'petrovic_00500000.gz', 'petrovic_01000000.gz', 'petrovic_01500000.gz',
                 'petrovic_02000000.gz',
                 'petrovic_02500000.gz', 'petrovic_03000000.gz']

    session = Session(tracker_on=False)
    folder = session.generate_temp_folder('DB2TXT')
    log_filename = path.join(folder, 'audit.log')
    print(log_filename)
    session.init_logger(filename=log_filename, std_level=simplelogger.DEBUG, file_level=simplelogger.DEBUG, profiling=True)

    streamer = TextGZipStreameSet(session, inputfiles_path, filenames, offset=offset)

    listener = SNAListener(session)
    listener.init(max_total=100000)

    streamer.register(listener)
    streamer.start()


    session.logger.info('Skipped {0} documents. Handled {1} retweets and {2} replies (total {3})'.format(listener.skipped,
                                                                                                         listener.retweets,
                                                                                                         listener.replies,
                                                                                                         listener.total  ))

    for c in listener.graph.giant_components(2):
        listener.graph.pprint(c, print_doc=listener.print_doc)

    print('Done.')
