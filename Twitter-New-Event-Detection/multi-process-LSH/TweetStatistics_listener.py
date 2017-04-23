from action_listener import Listener
import sys
import networkx as nx
import matplotlib.pyplot as plt

class SNAListener(Listener):

    total = 0
    replies = 0
    retweets = 0

    external = 0
    errors = 0
    loaded = 0
    new = 0
    undefined = 0

    skipped = 0
    graph = None
    max_total = -2
    offset = 0
    documents = {}

    skipped = {}

    def init(self, max_total=-1, offset=0):
        self.total = 0
        self.max_total = max_total
        self.replies = 0
        self.skipped = 0
        self.retweets = 0
        self.errors = 0
        self.offset = offset
        self.graph = InteractionGraph()
        self.documents = {}


    def act(self, data):
        self.session.logger.entry("SNAListener.act")

        ID = str(data.get('id_str', None))

        if data.get('json', None) is not None:
            data = data['json']

        if data.get('id_str', None) is None and data['status'] != 'Error':
            raise Exception("unexpected entry: " + str(data))

        self.total += 1
        if self.offset > self.total:
            if self.total % int(0.01 * self.offset) == 0:
                print('skip {0} to {1}'.format(self.total, self.offset))
            return True


        text = data.get('text', 'N/A')
        text = text.replace('\t', ' ').replace('\r', '. ').replace('\n', '. ')

        retweet = (data.get('retweeted_status', None) != None)
        reply = (data.get('in_reply_to_status_id', None) != None)

        to_id = None
        if reply:
            to_id = data['in_reply_to_status_id']
            self.replies += 1

        elif retweet:
            to_id = data['retweeted_status']['id_str']
            self.retweets += 1

        else:
            self.skipped += 1
            #print('{0}\t{1}\t{2}\t{3}\t{4}\t{5}'.format("N/A", id, topic, "N/A", status, text))
            #return True


        metadata = {}
        #ID = data['id_str']

        metadata['reply'] = reply
        metadata['retweet'] = retweet

        user = data.get('user', {'screen_name': None})
        metadata['user'] = user.get('screen_name', None)

        if metadata['user'] == None:
            metadata['user'] = user.get('id_str', None)

        metadata['timestamp'] = data.get('timestamp', None)

        metadata['text'] = text

        # self.session.logger.info(metadata['text'])
        count = self.total-self.offset
        if self.total % 5000 == 0:
            self.session.logger.debug('Loaded {} documents, processed {1}'.format(self.total, count))

        self.documents[ID] = metadata

        self.graph.add(ID, to_id)

        if self.documents.get(to_id, None) is None:
            self.documents[to_id] = { 'text' : 'N/A', 'retweet' : False, 'reply' : False}

        self.session.logger.exit("SNAListener.act")

        to_continue = self.max_total<-1 or count < self.max_total
        return to_continue

    def get_doc(self, id):
        return self.documents.get(id, None)

class InteractionGraph:

    u2u_mode = True
    nodes = []
    node2index = {}
    components = []
    node2component = {}
    edges = []

    def __init__(self, u2u_mode=True):
        self.u2u_mode = u2u_mode
        self.nodes = []
        self.node2index = {}
        self.components = []
        self.node2component = {}
        self.edges = []

    def _add(self, id):
        if id == None:
            return True, -1
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

        self.edges.append((index1, index2))

        if is_new1 and is_new2:
            self.components.append(Component())
            n = len(self.components)-1
            self.node2component[id1] = n
            self.components[n].add( index1 )
            if id2 is not None:
                self.node2component[id2] = n
                self.components[n].add( index2 )

        elif is_new1:
            n = self.node2component[id2]
            self.components[n].add( index1 )
            self.node2component[id1] = n
        else:
            n = self.node2component[id1]
            if id2 is not None:
                self.components[n].add( index2 )
                self.node2component[id2] = n

    def giant_components(self, number=-1):

        for c in sorted(self.components, key=Component.size, reverse=True):
            if number == -1:
                yield c
            elif number == 0:
                break
            else:
                yield c
                number -= 1

    def pprint(self, component, get_doc, out=sys.stdout, print_header=False):
        if print_header:
            out.write('#\tID\tComponent Size\ttype\tText\n')

        word_counts = {}

        for c in component.items:
            id = self.nodes[c]
            data = get_doc(id)

            text = "N/A"
            type = 'unknown'
            if data is not None:
                text = data['text']
                if(data['retweet']):
                    type = 'retweet'
                elif(data['reply']):
                    type = 'reply'


            text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')

            words = text.lower().split()
            for w in words:
                word_counts[w] = word_counts.get(w, 0) + 1

            out.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(str(component), id, component.size(), type, text))

    def write_component_header(self, component, out):
        word_counts = {}
        text = sorted(word_counts, key=lambda x: word_counts[x], reverse=True)
        text = ' '.join( text )
        out.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(str(component), '', component.size(), '', text))


    def pprint_edges(self, out=sys.stdout, print_header=False):
        if print_header:
            out.write('#\tID\tComponent Size\ttype\tText\n')

        for i1, i2 in self.edges:
            out.write('{0}\t{1}\ttype\n'.format( self.nodes[i1] , self.nodes[i2] ))


    def print_graph(self, get_doc, folder='./'):

        G = nx.DiGraph()

        for n in self.nodes:
            c = self.node2component[n]
            data = get_doc(n)

            G.add_node(n, {'group' : c} )

        for e in self.edges:
            G.add_edge( self.nodes[ e[0] ], self.nodes[ e[1] ])

        pos = nx.random_layout(G)
        nx.draw_networkx(G, pos, with_labels=False)

        nx.write_gexf(G, folder+'/interactions.gexf')
        #plt.show()


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
