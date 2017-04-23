from action_listener import TextGZipStreameSet, Listener
from session import Session
from simplelogger import simplelogger
from os import path
from SNA_listener import SNAListener
import sys
import codecs

if __name__ == '__main__':
    offset = 0
    inputfiles_path = 'C:\\data\\events_db\\petrovic'
    # filenames = ['relevance_judgments_00000000.gz']
    filenames = [   'petrovic_00000000.gz',
                    'petrovic_00500000.gz',
                    'petrovic_01000000.gz',
                    'petrovic_01500000.gz',
                    'petrovic_02000000.gz',
                    'petrovic_02500000.gz',
                    'petrovic_03000000.gz',
                    'petrovic_03500000.gz',
                    'petrovic_04000000.gz',
                    'petrovic_04500000.gz',
                    'petrovic_05000000.gz',
                    'petrovic_05500000.gz',
                    'petrovic_06000000.gz',
                    'petrovic_06500000.gz',
                    'petrovic_07000000.gz',
                    'petrovic_07500000.gz',
                    'petrovic_08000000.gz',
                    'petrovic_08500000.gz',
                    'petrovic_09000000.gz',
                    'petrovic_09500000.gz',
                    'petrovic_10000000.gz',
                    'petrovic_10500000.gz',
                    'petrovic_11000000.gz',
                    'petrovic_11500000.gz',
                    'petrovic_12000000.gz',
                    'petrovic_12500000.gz',
                    'petrovic_13000000.gz',
                    'petrovic_13500000.gz',
                    'petrovic_14000000.gz',
                    'petrovic_14500000.gz',
                    'petrovic_15000000.gz',
                    'petrovic_15500000.gz',
                    'petrovic_16000000.gz',
                    'petrovic_16500000.gz',
                    'petrovic_17000000.gz',
                    'petrovic_17500000.gz',
                    'petrovic_18000000.gz',
                    'petrovic_18500000.gz',
                    'petrovic_19000000.gz',
                    'petrovic_19500000.gz',
                    'petrovic_20000000.gz',
                    'petrovic_20500000.gz',
                    'petrovic_21000000.gz',
                    'petrovic_21500000.gz',
                    'petrovic_22000000.gz',
                    'petrovic_22500000.gz',
                    'petrovic_23000000.gz',
                    'petrovic_23500000.gz',
                    'petrovic_24000000.gz',
                    'petrovic_24500000.gz',
                    'petrovic_25000000.gz',
                    'petrovic_25500000.gz',
                    'petrovic_26000000.gz',
                    'petrovic_26500000.gz',
                    'petrovic_27000000.gz',
                    'petrovic_27500000.gz',
                    'petrovic_28000000.gz',
                    'petrovic_28500000.gz',
                    'petrovic_29000000.gz',
                    'petrovic_29500000.gz' ]

    session = Session(tracker_on=False)
    folder = session.generate_temp_folder('SNA')
    log_filename = path.join(folder, 'audit.log')
    print(log_filename)
    session.init_logger(filename=log_filename, std_level=simplelogger.DEBUG, file_level=simplelogger.DEBUG, profiling=True)

    streamer = TextGZipStreameSet(session, inputfiles_path, filenames, offset=offset)

    listener = SNAListener(session)
    listener.init(max_total=2000000, offset=8000000)
    #listener.init(max_total=2000, offset=100)

    #petrovic_09500000:{"contributors": null, "coordinates": null, "created_at": "Sat Jul 23 17:11:02 +0000 2011", "entities": {"hashtags": [{"indices": [0, 13], "text": "AmyWinehouse"}], "symbols": [], "urls": [], "user_mentions": []}, "favorite_count": 0, "favorited": false, "geo": null, "id": 94816805343866880, "id_str": "94816805343866880", "in_reply_to_screen_name": null, "in_reply_to_status_id": null, "in_reply_to_status_id_str": null, "in_reply_to_user_id": null, "in_reply_to_user_id_str": null, "is_quote_status": false, "lang": "en", "place": null, "retweet_count": 0, "retweeted": false, "source": "<a href=\"http://twitter.com\" rel=\"nofollow\">Twitter Web Client</a>", "text": "#AmyWinehouse died! :O Get ready for Roger Ebert's latest a-hole remark.", "timestamp": 1311430262.0, "truncated": false, "user": {"contributors_enabled": false, "created_at": "Sun Apr 17 16:08:46 +0000 2011", "default_profile": false, "default_profile_image": false, "description": "I'm a 20 year old film student trying to become famous.", "entities": {"description": {"urls": []}, "url": {"urls": [{"display_url": "groovysebaz.tumblr.com", "expanded_url": "http://groovysebaz.tumblr.com/", "indices": [0, 22], "url": "http://t.co/47NVv9Bp53"}]}}, "favourites_count": 38, "follow_request_sent": false, "followers_count": 35, "following": false, "friends_count": 59, "geo_enabled": false, "has_extended_profile": false, "id": 283592056, "id_str": "283592056", "is_translation_enabled": false, "is_translator": false, "lang": "en", "listed_count": 0, "location": "California", "name": "Sebastian Zambrano", "notifications": false, "profile_background_color": "C0DEED", "profile_background_image_url": "http://pbs.twimg.com/profile_background_images/234176499/earthworm-jim-is-groovy-.jpg", "profile_background_image_url_https": "https://pbs.twimg.com/profile_background_images/234176499/earthworm-jim-is-groovy-.jpg", "profile_background_tile": true, "profile_banner_url": "https://pbs.twimg.com/profile_banners/283592056/1401689591", "profile_image_url": "http://pbs.twimg.com/profile_images/540969771051663360/oJAwtObx_normal.jpeg", "profile_image_url_https": "https://pbs.twimg.com/profile_images/540969771051663360/oJAwtObx_normal.jpeg", "profile_link_color": "0084B4", "profile_sidebar_border_color": "C0DEED", "profile_sidebar_fill_color": "DDEEF6", "profile_text_color": "333333", "profile_use_background_image": true, "protected": false, "screen_name": "GroovySebaz", "statuses_count": 454, "time_zone": "Pacific Time (US & Canada)", "translator_type": "none", "url": "http://t.co/47NVv9Bp53", "utc_offset": -28800, "verified": false}}

    streamer.register(listener)

    streamer.start()

    session.logger.info(
        'Skipped {0} documents. Handled {1} retweets and {2} replies (total {3})'.format(listener.skipped,
                                                                                         listener.retweets,
                                                                                         listener.replies,
                                                                                         listener.total-offset))

    csv_file = path.join(folder, 'tweets.txt')
    output1 = codecs.open(csv_file, 'w+', encoding="utf-8")

    edges = path.join(folder, 'edges.txt')
    output2 = codecs.open(edges, 'w+', encoding="utf-8")

    count = 0
    for c in listener.graph.giant_components():
        #output1.write("component size : {0}\n".format (c.size()) )
        #listener.graph.pprint(c, get_doc=listener.get_doc, out=output1, print_header=(count == 0))
        listener.graph.write_component_header(c, out=output1)
        count += 1

    listener.graph.pprint_edges(out=output2, print_header=(count == 0))

    output1.close()
    output2.close()

    print('Errors:{0}, External: {1}, Loaded: {2}, New:{3}, Undefined:{4}'.format(
        listener.errors,
        listener.external,
        listener.loaded,
        listener.new,
        listener.undefined))

    listener.graph.print_graph(get_doc=listener.get_doc, folder=folder)

    print('Done: ', count, ' printed')
