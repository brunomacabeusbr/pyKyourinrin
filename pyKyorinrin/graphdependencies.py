import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.colors
from crawler import Crawler
import math


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
        # os nodes são todas as informações coletáveis
        for current_crawler in list_crawler:
            for current_crop in current_crawler.crop():
                if len(current_crop) > 0:
                    self.graph.add_node(current_crop)

        ###
        # criar edges da depedência para a info que colhe
        # os edges é um crawler ligando uma info à outra info, na ordem dependence -> crop
        for current_crawler in list_crawler:
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
             if len(current_crop) > 0 if current_dependence != current_crop]

        ###
        # definir posições dos nodes
        # isso só é necessário para desenhar o grafo
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

        # irá gerar a array com a ordem de depedencie
        # cada elemento será adicionado a uma nova array se depender apenas dos que já estão incluído nos leveis anteriores
        # por exemplo
        # [['a', 'b']] -> 'a' e 'b' dependem de ninguém
        # [['a', 'b'], ['c', 'd', 'e']] -> 'c', 'd' e 'e' depende de algum, ou todos, itens anteriores
        # [['a', 'b'], ['c', 'd', 'e'], ['f']] -> 'f' depende de algum, ou todos, elementos do segundo nível e/ou algum, ou todos, do primeiro nível

        level_dependence = []

        def level_dependence_expanded():
            return [i2 for i in level_dependence for i2 in i]

        def element_present(element):
            return element in level_dependence_expanded()

        level_dependence.append(  # armazenar infos de nível 0, ou seja, as infos bases
            set([i2
                 for i in list_crawler if i.have_dependencies() is False
                 for i2 in i.crop()])
        )

        total_infos = list(set([i2 for i in list_crawler for i2 in i.crop()]))

        level = 1
        while len(level_dependence_expanded()) != len(total_infos):
            level_dependence.append([])

            for current_crawler in list_crawler:
                for current_info in current_crawler.crop():
                    # verificar se a info já está presente na listagem
                    if element_present(current_info):
                        continue

                    # verificar depedencias dele
                    my_dependencies = [i for i in current_crawler.dependencies()]
                    # verificar se há várias rotas de depedencia
                    multiple_dependence_routes = (type(my_dependencies[0]) == tuple)
                    if multiple_dependence_routes:
                        # se tiver multiplas rotas, vai verificar se alguma delas é alcançável com os dados já disponíveis
                        for current_route in my_dependencies:
                            my_check_dep = [element_present(i) for i in current_route]
                            if False not in my_check_dep:
                                break
                    else:
                        my_check_dep = [element_present(i) for i in my_dependencies]
                    if False not in my_check_dep:
                        level_dependence[level].append(current_info)

            level += 1

        level = 1
        for i in level_dependence:
            self.pos.update(get_points(level, i))
            level += 1

        # todo: primeiro momento vai desenhar o grafo usando a alternativa 1, depoi vai desenhar usando a alternativa 2,
        # todo: desenhar legenda para tirar as labels de cima das arestas

    def draw(self):
        edges, colors = zip(*nx.get_edge_attributes(self.graph, 'color').items())

        nx.draw(self.graph, pos=self.pos, with_labels=True,
                font_size=10, font_color='r',
                node_color='black', node_size=1000, alpha=0.5,
                edgelist=edges, edge_color=colors)

        # todo: talvez haja um meio melhor de fazer a legenda
        # todo: caso dois edges tenham a mesma rota, um deles vai ficar escondido
        #import matplotlib.patches as mpatches
        #edges_patches = []
        #for i in Crawler.__subclasses__():
        #    edges_patches.append(mpatches.Patch(color=i.my_color, label=i.name()))
#
        #plt.legend(handles=edges_patches)

        plt.savefig("graph.png")
        plt.show()

gd = GraphDependencies()


class GraphDependenciesOfPrimitiveRow:
    def __init__(self, db, primitive_id, primitive_name):
        self.primitive_id = primitive_id
        self.primitive_name = primitive_name
        self.db = db
        self.gd = gd.graph.copy()
        self.pos = gd.pos

        ###
        # apagar edges de crawlers já usados
        crawler_list_used = self.db.crawler_list_used(self.primitive_id, self.primitive_name)
        for k in crawler_list_used.keys():
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
        edges, colors = None, None
        edges_colors_items = nx.get_edge_attributes(self.gd, 'color').items()
        if len(edges_colors_items):
            edges, colors = zip(*edges_colors_items)

        nodes, node_color = zip(*nx.get_node_attributes(self.gd, 'node_color').items())

        nx.draw(self.gd, pos=self.pos, with_labels=True,
                font_size=10, font_color='r',
                nodelist=nodes, node_color=node_color, node_size=1000, alpha=0.5,
                edgelist=edges, edge_color=colors)

        # todo: talvez haja um meio melhor de fazer a legenda
        # todo: caso dois edges tenham a mesma rota, um deles vai ficar escondido
        import matplotlib.patches as mpatches
        edges_patches = []
        for i in Crawler.__subclasses__():
            edges_patches.append(mpatches.Patch(color=i.my_color, label=i.name()))

        plt.legend(handles=edges_patches)

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
