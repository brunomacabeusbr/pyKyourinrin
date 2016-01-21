import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.colors
from crawler import Crawler


class GraphDependencies:
    def __init__(self, primitive_name):
        self.primitive_name = primitive_name
        self.graph = nx.DiGraph()

        self.list_crawler = [i for i in Crawler.__subclasses__() if 'primitive_' + self.primitive_name in i.primitive_required()]

        ###
        # definir cores dos crawler
        # cada crawler tem uma cor única. a cor é usada na hora de desenhar o edge
        import random

        colors_available = list(matplotlib.colors.cnames.keys())

        for i in self.list_crawler:
            while True:
                color_choice = random.choice(colors_available)
                colors_available.remove(color_choice)
                r, g, b = matplotlib.colors.hex2color(matplotlib.colors.cnames[color_choice])
                if r > 0.7 and g > 0.7 and b > 0.7:
                    continue
                break
            i.my_color = color_choice

        ###
        # criar node
        # os nodes são todas as informações coletáveis do crawler e as nativas da primitive
        import xml.etree.ElementTree as ET

        info_primitive = []
        info_crawler = []

        path_pykyorinrin = os.path.dirname(__file__)
        xml_root = ET.parse(path_pykyorinrin + '/primitives/' + self.primitive_name + '.xml').getroot()
        for current_info in xml_root.findall('column'):
            info_primitive.append(current_info.find('name').text)
            self.graph.add_node(current_info.find('name').text)

        for current_crawler in self.list_crawler:
            for current_crop in [i for i in current_crawler.crop() if len(i) > 0]:
                if type(current_crop) is tuple:
                    if current_crop[1] != 'primitive_' + self.primitive_name:
                        continue
                    else:
                        current_crop = current_crop[0]

                if current_crop not in info_primitive:
                    info_crawler.append(current_crop)
                    self.graph.add_node(current_crop)

        ###
        # criar edges da depedência para a info que colhe
        # os edges é um crawler ligando uma info à outra info, na ordem dependence -> crop
        for current_crawler in self.list_crawler:
            if current_crawler.have_dependencies() is False:
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
             for current_dependence in dependencies for current_crop in current_crawler.crop()
             if len(current_crop) > 0 if current_dependence != current_crop
             if current_crop in info_primitive + info_crawler if current_dependence in info_primitive + info_crawler]

        ###
        # definir posições dos nodes
        # isso só é necessário para desenhar o grafo
        # há dois níveis: centro e extremidade
        # - no centro, ficam as infos da primitive
        # - na extremidade, ficam as infos do crawler
        import math
        self.pos = dict()

        def get_points(r, nodes):
            pos = dict()

            teta = 360 / len(nodes)
            teta = math.radians(teta)

            x = 0
            for i in nodes:
                pos[i] = (
                    (r * 1) * (math.cos(teta * x)),
                    (r * 1) * (math.sin(teta * x))
                )
                x += 1

            return pos

        self.pos.update(get_points(1, info_primitive))
        if len(info_crawler) > 0:
            self.pos.update(get_points(2, info_crawler))

    def draw(self):
        plt.suptitle('Primitive {}'.format(self.primitive_name), fontsize=14, fontweight='bold')

        # Grade
        import matplotlib.gridspec as gridspec
        gs = gridspec.GridSpec(1, 4)
        gs.update(top=1, bottom=0, left=0, right=1, wspace=0)

        # Grafo
        # todo: caso dois edges tenham a mesma rota, um deles vai ficar escondido
        plt.subplot(gs[-1:, :-1])
        edges, colors = zip(*nx.get_edge_attributes(self.graph, 'color').items())

        nx.draw(self.graph, pos=self.pos, with_labels=True,
                font_size=10, font_color='r',
                node_color='black', node_size=1000, alpha=0.5,
                edgelist=edges, edge_color=colors)

        # Legenda
        plt.subplot(gs[-1:, -1])

        import matplotlib.patches as mpatches
        plt.legend(
            handles=[
                mpatches.Patch(color=i.my_color, label=i.name())
                for i in self.list_crawler
            ]
        )

        # Plot
        plt.axis('off')
        plt.savefig("graph.png")
        plt.show()

import os

GraphDependencies.primitive_graphs = {
    current_xml[:-4]: GraphDependencies(current_xml[:-4])
    for current_xml in os.listdir(os.path.dirname(__file__) + '/primitives/')
}


class GraphDependenciesOfPrimitiveRow:
    def __init__(self, db, primitive_id, primitive_name):
        self.primitive_id = primitive_id
        self.primitive_name = primitive_name
        self.db = db
        self.gd = GraphDependencies.primitive_graphs[primitive_name].graph.copy()
        self.pos = GraphDependencies.primitive_graphs[primitive_name].pos
        self.list_crawler = GraphDependencies.primitive_graphs[primitive_name].list_crawler

        ###
        # apagar edges de crawlers já usados
        for k in self.db.crawler_list_used(self.primitive_id, self.primitive_name).keys():
            [self.gd.remove_edge(*i) for i in self.gd.edges()
             if self.gd.get_edge_data(*i)['crawler'].name() == k]

        ###
        # marcar nodes com dados já obtidos
        infos = self.db.get_primitive_row_info_all(self.primitive_id, self.primitive_name)
        for k, v in infos.items():
            if k not in self.gd.node:
                continue

            if v is not None:
                self.gd.node[k]['node_color'] = 'blue'
            else:
                self.gd.node[k]['node_color'] = 'black'

    def draw(self):
        plt.suptitle('Primitive {} #{}'.format(self.primitive_name, self.primitive_id), fontsize=14, fontweight='bold')

        # Grade
        import matplotlib.gridspec as gridspec
        gs = gridspec.GridSpec(1, 4)
        gs.update(top=1, bottom=0, left=0, right=1, wspace=0)

        # Grafo
        # todo: caso dois edges tenham a mesma rota, um deles vai ficar escondido
        plt.subplot(gs[-1:, :-1])
        edges, edges_colors = None, None
        edges_colors_items = nx.get_edge_attributes(self.gd, 'color').items()
        if len(edges_colors_items):
            edges, edges_colors = zip(*edges_colors_items)

        nodes, node_color = zip(*nx.get_node_attributes(self.gd, 'node_color').items())

        nx.draw(self.gd, pos=self.pos, with_labels=True,
                font_size=10, font_color='r',
                nodelist=nodes, node_color=node_color, node_size=1000, alpha=0.5,
                edgelist=edges, edge_color=edges_colors)

        # Legenda
        plt.subplot(gs[-1:, -1])
        crawler_list_status = self.db.crawler_list_status(self.primitive_id, self.primitive_name)

        import matplotlib.patches as mpatches
        plt.legend(
            handles=[
                mpatches.Patch(color=i.my_color, label=(i.name(), i.name() + ' ✓', i.name() + ' ×')[crawler_list_status[i.name()]])
                for i in self.list_crawler
            ]
        )

        # Plot
        plt.axis('off')
        plt.savefig("graph_primitive_row.png")
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
            i.harvest(**{'primitive_' + self.primitive_name: self.primitive_id})

            # Conseguiu colher o dado desejado? Se sim, para o loop
            if self.db.get_dependencies(self.primitive_id, self.primitive_name, target)[target] is not None:
                break

        # Retornar se teve sucesso em recolher a depedência desejada
        if self.db.get_dependencies(self.primitive_id, self.primitive_name, target)[target] is not None:
            return True
        else:
            return False

        # TODO: chamar os crawlers da lista crawlers_to_target com thread e, assim que o target for alcançado,
        # parar imediatamente todas as demais thread que estiverem rodando

        # só vai terminar essa função quando todas as threads forem finalizadas
