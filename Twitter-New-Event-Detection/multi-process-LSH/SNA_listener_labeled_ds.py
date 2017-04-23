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
    documents = {}

    skipped = {}

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

        if data.get('status', None) is None:
            self.external += 1
        elif data['status'] == 'Loaded':
            self.loaded += 1
        elif data['status'] == 'Error':
            self.errors += 1
        else: #if data['status'] == 'New':
            self.new += 1

        ID = str(data.get('_id', None))


        print('ID:\t'+ID)
        topic = data.get('topic_id', '')
        data['topic'] = topic
        status = data.get('status', '')

        if data.get('json', None) is not None:
            data = data['json']

        if data.get('id_str', None) is None and data['status'] != 'Error':
            raise Exception("unexpected entry: " + str(data))

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
        metadata['topic'] = topic

        user = data.get('user', {'screen_name': None})
        metadata['user'] = user.get('screen_name', None)

        if metadata['user'] == None:
            metadata['user'] = user.get('id_str', None)

        metadata['timestamp'] = data.get('timestamp', None)
        metadata['status'] = status

        metadata['text'] = text

        # self.session.logger.info(metadata['text'])
        if self.total % 5000 == 0:
            self.session.logger.debug('Loaded {} documents'.format(self.total))

        self.total += 1
        self.documents[ID] = metadata

        self.graph.add(ID, to_id)

        if self.documents.get(to_id, None) is None:
            self.documents[to_id] = { 'status' : 'unknown11', 'topic' : topic, 'text' : 'N/A', 'retweet' : False, 'reply' : False}

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
                yield
                number -= 1

    def pprint(self, component, get_doc, out=sys.stdout, print_header=False):
        if print_header:
            out.write('#\tID\tTopic\tComponent Size\tStatus\ttype\tText\n')

        for c in component.items:
            id = self.nodes[c]
            data = get_doc(id)

            topic = "Why Empty?!"
            text = "N/A"
            status = "EXTERNAL"
            type = 'unknown'
            if data is not None:
                topic = data['topic']
                text = data['text']
                status = data['status']
                if(data['retweet']):
                    type = 'retweet'
                elif(data['reply']):
                    type = 'reply'
                elif status == 'Error':
                    type = 'Error'
                elif status == 'Loaded':
                    type = 'original'

            text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
            out.write('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\n'.format(str(component), id, topic, component.size(), status, type, text))


    def print_graph(self, get_doc):

        G = nx.DiGraph()

        for n in self.nodes:
            c = self.node2component[n]
            data = get_doc(n)
            topic = -1
            if data is not None:
                topic = data['topic']
            G.add_node(n, {'group' : c, 'topic' : topic} )

        for e in self.edges:
            G.add_edge( self.nodes[ e[0] ], self.nodes[ e[1] ])

        pos = nx.random_layout(G)
        nx.draw_networkx(G, pos, with_labels=False)

        nx.write_gexf(G, "c:/temp/interactions.gexf")
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
