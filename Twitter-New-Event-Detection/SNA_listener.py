from action_listener import Listener
from pprint import pprint
import sys

class SNAListener(Listener):

    total = 0
    replies = 0
    retweets = 0
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
        self.graph = InteractionGraph()
        self.documents = {}


    def act(self, data):
        self.session.logger.entry("SNAListener.act")

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

    def print_doc(self, id, output=sys.stdout):
        data = self.documents.get(id, None)

        if data != None:
            output.write('{0}: {1}\n'.format(id, data['text']))
        else:
            output.write('{0}: N/A\n'.format(id))


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

    def giant_components(self, number):
        result = []

        for c in sorted(self.components, key=Component.size, reverse=True):
            result.append(c)
            if number==0:
                return result
            number -= 1

        return []

    def pprint(self, component, print_doc=None, out=sys.stdout):
        out.write('Component size: {0}:\n'.format(component.size()))
        for c in component.items:
            if print_doc != None:
                print_doc(self.nodes[c], out)
            else:
                out.write(str(c))
                out.write('\n')


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
