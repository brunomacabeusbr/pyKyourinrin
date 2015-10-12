import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.colors
from crawler import Crawler


class GraphDependencies:
    def __init__(self):
        self.graph = nx.DiGraph()

        list_crawler = Crawler.__subclasses__()
        list_dates_base = [i.crop() for i in list_crawler if i.dependencies() == ''] # pegar todos os dados que não tem depedencias
        list_dates_base = [x for xs in list_dates_base for x in xs] # remover das tuplas

        ## definir cores por crawler

        colors = {}
        colors_available = list(matplotlib.colors.cnames.keys())

        x = 0
        for i in list_crawler:
            colors[i] = colors_available[x]
            x += 1

        ## criar node

        for i in list_crawler:
            for i2 in i.crop():
                self.graph.add_node(i2)

        ## crir edges da depedência para a info que colhe

        for i in list_crawler:
            for i2 in i.dependencies():
                for i3 in i.crop():
                    self.graph.add_edge(i2, i3, color=colors[i], crawler=i, crawler_name=i.name())

        ## para o draw: definir posições dos nodes

        pos = {}

        def get_x_possible(x, level, depedencies):
            if (x, level) in pos:
                return get_x_possible(x + 1, level, depedencies)
            else:
                ok = True
                dependencia_no_mesmo_y = False
                for i in range(level):
                    if not dependencia_no_mesmo_y:
                        if (x, i) in pos and pos[x, i] in depedencies:
                            dependencia_no_mesmo_y = True
                    else:
                        if (x, i) in pos:
                            ok = False
                            break

                if ok:
                    return x
                else:
                    return get_x_possible(x + 1, level, depedencies)

        def add_pos_base(name, depedencies):
            add_pos(0, name, depedencies)

        def add_pos(level, name, depedencies):
            pos[(get_x_possible(0, level, depedencies), level)] = name

        for i in list_crawler:
            for i2 in i.crop():
                if i.dependencies() == '':
                    add_pos_base(i2, i.dependencies())
                else:
                    mypaths = []
                    for i3 in list_dates_base:
                        for i4 in nx.all_simple_paths(self.graph, i3, i2):
                            try:
                                mypaths.append(len(i4))
                            except:
                                pass

                    add_pos(max(mypaths), i2, i.dependencies())

        self.pos_invert = {v: k for k, v in pos.items()}

    def draw(self):
        edges, colors = zip(*nx.get_edge_attributes(self.graph, 'color').items())

        nx.draw(self.graph, pos=self.pos_invert, with_labels=True,
                font_size=10, font_color='r',
                node_color='black', node_size=1000,
                edgelist=edges, edge_color=colors)
        nx.draw_networkx_edge_labels(self.graph, pos=self.pos_invert, edge_labels=nx.get_edge_attributes(self.graph, 'crawler_name'), label_pos=0.85, font_size=8)
        # todo: em edge_labels, preciso separar por vírgula caso haja dois ou mais crawlers que levem para a mesma info
        plt.savefig("graph.png")
        plt.show()

gd = GraphDependencies()


class GraphDependenciesOfThisPeople:
    def __init__(self, db, id):
        self.id = id
        self.db = db
        self.gd = gd
        self.pos_invert = gd.pos_invert

        # apagar edges de crawlers em que não se obteve sucesso ao tentar usar
        crawler_status = self.db.crawler_status(self.id)
        for k, v in crawler_status.items():
            if v == -1:
                for i in self.gd.graph.edges():
                    edge_crawler = self.gd.graph.get_edge_data(*i)['crawler']
                    if edge_crawler == k:
                        self.gd.graph.remove_edge(*i)

        # marcar nodes com dados já obtidos
        people_status = self.db.get_people_info(self.id)
        for k, v in people_status.items():
            if v != None:
                self.gd.graph.node[k]['node_color'] = 'blue'
            else:
                self.gd.graph.node[k]['node_color'] = 'black'

        #self.draw()

    def draw(self):
        edges, colors = zip(*nx.get_edge_attributes(self.gd.graph, 'color').items())
        nodes, node_color = zip(*nx.get_node_attributes(self.gd.graph, 'node_color').items())

        nx.draw(self.gd.graph, pos=self.pos_invert, with_labels=True,
                font_size=10, font_color='r',
                nodelist=nodes, node_color=node_color, node_size=1000,
                edgelist=edges, edge_color=colors)
        nx.draw_networkx_edge_labels(self.gd.graph, pos=self.pos_invert, edge_labels=nx.get_edge_attributes(self.gd.graph, 'crawler_name'), label_pos=0.85, font_size=8)
        # todo: em edge_labels, preciso separar por vírgula caso haja dois ou mais crawlers que levem para a mesma info
        plt.savefig("graph.png")
        plt.show()

    def harvest_dependence(self, target):
        # buscar os crawlers que levam ao node target
        crawlers_to_target = []
        for i in self.gd.graph.in_edges(target):
            edge_crawler = self.gd.graph.get_edge_data(*i)['crawler']
            if not edge_crawler in crawlers_to_target:
                crawlers_to_target.append(edge_crawler)

        # todo: falta considerar o caso de que não haja nenhuma rota possível

        # vamos usar os crawlers que levam ao node target
        # repare que esse código leva à recursividade, pois se uma das depedências desses crawlers não estiverem satisfeitas,
        # será chamado novamente esse mesmo código

        for i in crawlers_to_target:
            i.harvest(id=self.id) # todo: tomar cuidado com os crawlers base, da qual não possuem o parâmetro "id"
            # todo: parar esse loop caso haja sucesso em pegar a depedência

        # TODO: chamar os crawlers da lista crawlers_to_target com thread e, assim que o target for alcançado,
        # parar imediatamente todas as demais thread que estiverem rodando

        # só vai terminar essa função quando todas as threads forem finalizadas