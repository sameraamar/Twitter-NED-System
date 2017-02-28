from action_listener import Listener
from pprint import pprint
import sys

class SNAListener(Listener):

    total = 0
    replies = 0
    retweets = 0
    errors = 0
    skipped = 0
    graph = None
    max_total = -2
    documents = {}


    def init(self, max_total=-1):
        self.total = 0
        self.max_total = max_total
        self.replies = 0
        self.skipped = 0
        self.retweets = 0
        self.errors = 0
        self.graph = InteractionGraph()
        self.documents = {}


    def act(self, data):
        self.session.logger.entry("SNAListener.act")

        if data.get('json', None) is not None:
            topic = data.get('topic_id', '')
            data = data['json']
            data['topic'] = topic

        if data.get('id_str', None) is None:
            self.errors += 1
            return True

        retweet = (data.get('retweeted_status', None) != None)
        reply = (data.get('in_reply_to_status_id', None) != None)


        if reply:
            to_id = data['in_reply_to_status_id']
            self.replies += 1

        elif retweet:
            to_id = data['retweeted_status']['id_str']
            self.retweets += 1

        else:
            self.skipped += 1
            return True


        metadata = {}
        ID = data['id_str']

        metadata['reply'] = reply
        metadata['retweet'] = retweet
        metadata['topic'] = data['topic']

        metadata['user'] = data['user'].get('screen_name', None)

        metadata['user'] = data['user'].get('screen_name', None)
        if metadata['user'] == None:
            metadata['user'] = data['user']['id_str']
        metadata['timestamp'] = data['timestamp']

        metadata['text'] = data['text'].replace('\t', ' ').replace('\n', '. ')

        # self.session.logger.info(metadata['text'])
        if self.total % 5000 == 0:
            self.session.logger.debug('Loaded {} documents'.format(self.total))

        self.total += 1
        self.documents[ID] = metadata

        self.graph.add(ID, to_id)

        self.session.logger.exit("SNAListener.act")

        to_continue = self.max_total<-1 or self.total < self.max_total
        return to_continue

    def get_doc(self, id):
        return self.documents.get(id, None)

class InteractionGraph:

    u2u_mode = True
    nodes = []
    node2index = {}
    components = []
    node2component = {}

    def __init__(self, u2u_mode=True):
        self.u2u_mode = u2u_mode
        self.nodes = []
        self.node2index = {}
        self.components = []
        self.node2component = {}

    def _add(self, id):
        is_new = False
        index = self.node2index.get(id, -1)
        if index == -1:
            is_new = True
            self.nodes.append(id)
            index = len(self.nodes)-1
            self.node2index[id] = index
        return is_new, index

    def add(self, id1, id2):
        is_new1, index1 = self._add(id1)

        is_new2, index2 = self._add(id2)

        if is_new1 and is_new2:
            self.components.append(Component())
            n = len(self.components)-1
            self.node2component[id1] = n
            self.node2component[id2] = n
            self.components[n].add( index1 )
            self.components[n].add( index2 )

        elif is_new1:
            n = self.node2component[id2]
            self.components[n].add( index1 )
            self.node2component[id1] = n
        else:
            n = self.node2component[id1]
            self.components[n].add( index2 )
            self.node2component[id2] = n

    def giant_components(self, number=-1):

        for c in sorted(self.components, key=Component.size, reverse=True):
            if number == -1:
                yield c
            elif number == 0:
                break
            else:
                yield
                number -= 1



    def pprint(self, component, get_doc, out=sys.stdout):
        out.write('ID\tTopic\tComponent Size\tText\n')

        for c in component.items:
            id = self.nodes[c]
            data = get_doc(id)

            topic = text = "N/A"
            if data is not None:
                topic = data['topic']
                text = data['text']

            out.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(str(component), id, topic, component.size(), text))






class Component:
    items = set()

    def __init__(self):
        self.items = set()

    def size(self):
        return len(self.items)

    def merge_with(self, other):
        self.items = self.items + other.items

    def add(self, node):
        self.items.add( node )
