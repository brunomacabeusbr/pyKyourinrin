import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.colors
from crawler import Crawler


class GraphDependencies:
    def __init__(self):
        self.graph = nx.DiGraph()

        list_crawler = Crawler.__subclasses__()

        ###
        # definir cores por crawler
        # cada crawler tem uma cor única. a cor é usada na hora de desenhar o edge
        import random

        colors_available = list(matplotlib.colors.cnames.keys())

        for i in list_crawler:
            color_choice = random.choice(colors_available)
            colors_available.remove(color_choice)
            i.my_color = color_choice

        ###
        # criar node
        # os nodes são todas as informações coletáveis
        for current_crawler in list_crawler:
            for current_crop in current_crawler.crop():
                self.graph.add_node(current_crop)

        ###
        # crir edges da depedência para a info que colhe
        # os edges é um crawler ligando uma info à outra info, na ordem dependence -> crop
        for current_crawler in list_crawler:
            if len(current_crawler.dependencies()) == 0:
                continue

            crawler_infos = {'color': current_crawler.my_color, 'crawler': current_crawler, 'crawler_name': current_crawler.name()}

            dependencies = []
            if type(current_crawler.dependencies()[0]) == tuple:
                for i2 in current_crawler.dependencies():
                    for i3 in i2:
                        if i3 not in dependencies:
                            dependencies.append(i3)
            else:
                dependencies = current_crawler.dependencies()

            [self.graph.add_edge(current_dependence, current_crop, **crawler_infos)
             for current_dependence in dependencies for current_crop in current_crawler.crop()]

        ###
        # definir posições dos nodes
        # isso só é necessário para desenhar o grafo
        list_dates_base = [i.crop() for i in list_crawler if i.dependencies() == ''] # pegar todos as infos que podem ser alcançadas sem depedência
        list_dates_base = [x for xs in list_dates_base for x in xs] # remover das tuplas

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
        self.gd = gd.graph.copy()
        self.pos_invert = gd.pos_invert

        ###
        # apagar edges de crawlers já usados
        crawler_list_used = self.db.crawler_list_used(self.id)
        for k in crawler_list_used.keys():
            [self.gd.remove_edge(*i) for i in self.gd.edges()
             if self.gd.get_edge_data(*i)['crawler'].name() == k]

        ###
        # marcar nodes com dados já obtidos
        people_status = self.db.get_people_info_all(self.id)
        for k, v in people_status.items():
            if k not in self.gd.node:
                continue

            if v is not None:
                self.gd.node[k]['node_color'] = 'blue'
            else:
                self.gd.node[k]['node_color'] = 'black'

    def draw(self):
        edges, colors = zip(*nx.get_edge_attributes(self.gd, 'color').items())
        nodes, node_color = zip(*nx.get_node_attributes(self.gd, 'node_color').items())

        nx.draw(self.gd, pos=self.pos_invert, with_labels=True,
                font_size=10, font_color='r',
                nodelist=nodes, node_color=node_color, node_size=1000,
                edgelist=edges, edge_color=colors)
        nx.draw_networkx_edge_labels(self.gd, pos=self.pos_invert, edge_labels=nx.get_edge_attributes(self.gd, 'crawler_name'), label_pos=0.85, font_size=8)
        # todo: em edge_labels, preciso separar por vírgula caso haja dois ou mais crawlers que levem para a mesma info
        plt.savefig("graph_people.png")
        plt.show()

    def is_dependence_reachable(self, target, exclude_crawler=None):
        if exclude_crawler is None:
            return len(self.gd.in_edges(target)) != 0

        for i in self.gd.in_edges(target):
            if self.gd.get_edge_data(*i)['crawler'].name() == exclude_crawler:
                continue

            return True

        return False

    def harvest_dependence(self, target):
        # buscar os crawlers que levam ao node target
        crawlers_to_target = []
        for i in self.gd.in_edges(target):
            edge_crawler = self.gd.get_edge_data(*i)['crawler']
            if not edge_crawler in crawlers_to_target:
                crawlers_to_target.append(edge_crawler)

        # vamos usar os crawlers que levam ao node target
        # repare que esse código leva à recursividade, pois se uma das depedências desses crawlers não estiverem satisfeitas,
        # será chamado novamente esse mesmo código

        for i in crawlers_to_target:
            i.harvest(id=self.id)

            # Conseguiu colher o dado desejado? Se sim, para o loop
            if self.db.get_dependencies(self.id, target)[target] is not None:
                break

        # Retornar se teve sucesso em recolher a depedência desejada
        if self.db.get_dependencies(self.id, target)[target] is not None:
            return True
        else:
            return False

        # TODO: chamar os crawlers da lista crawlers_to_target com thread e, assim que o target for alcançado,
        # parar imediatamente todas as demais thread que estiverem rodando

        # só vai terminar essa função quando todas as threads forem finalizadas
